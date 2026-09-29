/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cloudprovider

import (
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// DefaultPoolAtMaxCooldown is how long a NodePool is held back from provisioning after the broker
// refused an add because the pool is already at its platform maximum. Overridable with
// RAFAY_POOL_AT_MAX_COOLDOWN.
//
// The refusal means the platform counts more nodes in the pool than Karpenter does — a node added
// from the console that has not joined yet, or a maximum lowered in the catalog that the next
// config sync (10 min) has not rendered as limits.nodes. Both heal on their own (node adoption,
// config sync); the cooldown just stops Karpenter from re-asking every few minutes meanwhile.
const DefaultPoolAtMaxCooldown = 5 * time.Minute

// poolAtMaxDetailMarkers are the phrases edge-broker puts in the FAILED detail of an add
// operation it refused because the pool is at its maximum (scaling.max / maxNodeCount). The first
// is the contract with edge-broker's karpenterPoolAtMaxReason (pkg/context/
// karpenter_backend_firstclass.go); the second is the wording brokers used before partial batch
// acceptance, kept so an old broker still triggers the backoff. Matched case-insensitively.
var poolAtMaxDetailMarkers = []string{"pool at maximum", "is at its maximum"}

// permanentRefusalDetailMarkers are the phrases edge-broker puts in the detail of an operation it
// refused for a reason no retry can fix. The broker starts the detail with one of the exact
// prefixes of the cross-repo contract:
//
//	pool at maximum      – scaling.max / maxNodeCount reached (also in poolAtMaxDetailMarkers)
//	pool at minimum      – a remove would drop below scaling.min (old brokers FAIL the op with it;
//	                       current brokers report it SUCCEEDED with a "node not retired" detail)
//	pool not found       – the pool is not on the cluster / catalog any more
//	pool sku mismatch    – the pool's SKU is not the one the NodeClaim selected
//	pool not auto-scaling – the pool has no scaling block / autoscaling is off
//	pool precondition    – PaaS profile/service not configured, catalog variable missing
//
// The remaining entries are the wordings brokers used before the prefixes existed, kept so an
// old broker still gets the same treatment. Matched case-insensitively anywhere in the detail,
// because the batcher prefixes the detail with the batch kind ("batch add: ...").
var permanentRefusalDetailMarkers = append([]string{
	"pool at minimum",
	"pool not found",
	"pool sku mismatch",
	"pool not auto-scaling",
	"pool precondition",
	// legacy wordings
	"is at its minimum",
	"is not on cluster",
	"on the cluster but",
	"has no scaling block",
	"service pool not configured",
	"not found on compute instance",
}, poolAtMaxDetailMarkers...)

// notRetiredDetailPrefix is what edge-broker puts at the start of the detail of a remove
// operation it reported SUCCEEDED without retiring a machine (the count could not be lowered: pool
// at scaling.min, unknown pool, SKU mismatch, pool not auto-scaling). The machine behind the
// NodeClaim is still running; Delete() still converges (NodeClaimNotFound) but warns.
const notRetiredDetailPrefix = "node not retired"

func containsAnyFold(detail string, markers []string) bool {
	d := strings.ToLower(detail)
	for _, m := range markers {
		if strings.Contains(d, m) {
			return true
		}
	}
	return false
}

// IsPoolAtMaxDetail reports whether a FAILED add operation's detail says the pool is at its
// platform maximum, i.e. the failure is not transient and reprovisioning right away would only
// be refused again.
func IsPoolAtMaxDetail(detail string) bool {
	return containsAnyFold(detail, poolAtMaxDetailMarkers)
}

// IsPermanentRefusalDetail reports whether an operation's FAILED detail names a refusal a retry
// cannot fix (see permanentRefusalDetailMarkers). Every pool-at-maximum detail is one; the batch
// failure handler holds the pool back for all of them instead of letting Karpenter re-create the
// NodeClaim every few minutes, and Delete() stops re-sending a remove refused this way.
func IsPermanentRefusalDetail(detail string) bool {
	return containsAnyFold(detail, permanentRefusalDetailMarkers)
}

// IsNotRetiredDetail reports whether a SUCCEEDED remove operation's detail says the broker
// applied no catalog change and the machine is still running (notRetiredDetailPrefix).
func IsNotRetiredDetail(detail string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(detail)), notRetiredDetailPrefix)
}

// PoolBackoff remembers, per NodePool, until when the broker's "pool at maximum" refusal should
// keep Karpenter from provisioning into that pool. The batch failure handler marks a pool when the
// broker reports the refusal; GetInstanceTypes renders the pool's offerings unavailable while the
// mark is active, which is the standard Karpenter way to tell the scheduler "not here, not now"
// (the pods stay pending with a "no instance type has the required offering" message, and no
// NodeClaim is created). Entries expire on their own; nothing has to clear them.
//
// PoolBackoff is also the only object main.go hands to both the batch failure handler and the
// CloudProvider, so it doubles as their channel for per-operation outcomes: the handler records
// every terminal FAILED operation the poller reports (MarkFailedOp) and Delete() consults that
// record (FailedOp) to tell "the add never landed a machine" and "the broker permanently refused
// this remove" from "still in progress". Those records are independent of the cooldown: a disabled
// backoff (cooldown <= 0) still keeps them.
//
// A nil *PoolBackoff is a valid, inert instance: Mark and MarkFailedOp are no-ops, Until never
// reports active and FailedOp never reports a record.
type PoolBackoff struct {
	mu       sync.Mutex
	cooldown time.Duration
	until    map[string]time.Time
	failed   map[string]failedOp
	now      func() time.Time
}

// failedOp is a terminal FAILED operation as the batch failure handler saw it.
type failedOp struct {
	detail string
	at     time.Time
}

// failedOpRetention bounds how long a FAILED operation is remembered. It only has to outlive the
// seconds between the failure handler recording it and Karpenter's next Delete() reconcile (which
// runs every ~5s while a NodeClaim is terminating); the generous window covers a slow termination.
// It matches the batcher's retention of SUCCEEDED operations.
const failedOpRetention = 2 * time.Hour

// NewPoolBackoff returns a PoolBackoff holding each marked pool for cooldown. A non-positive
// cooldown disables the backoff (Mark becomes a no-op), which is the escape hatch for a
// deployment that wants the old delete-and-reprovision-immediately behaviour.
func NewPoolBackoff(cooldown time.Duration) *PoolBackoff {
	return &PoolBackoff{cooldown: cooldown, until: map[string]time.Time{}, failed: map[string]failedOp{}, now: time.Now}
}

// Cooldown returns the configured hold duration.
func (b *PoolBackoff) Cooldown() time.Duration {
	if b == nil {
		return 0
	}
	return b.cooldown
}

// Mark holds pool back for the cooldown from now, extending an earlier mark, and returns the
// time the hold ends. The zero time means nothing was recorded (nil receiver, empty pool name or
// a disabled backoff).
func (b *PoolBackoff) Mark(pool string) time.Time {
	if b == nil || b.cooldown <= 0 || pool == "" {
		return time.Time{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	until := b.now().Add(b.cooldown)
	b.until[pool] = until
	return until
}

// Until reports whether pool is currently held back and, if so, until when. An expired mark is
// forgotten on the way out.
func (b *PoolBackoff) Until(pool string) (time.Time, bool) {
	if b == nil {
		return time.Time{}, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	until, ok := b.until[pool]
	if !ok {
		return time.Time{}, false
	}
	if !b.now().Before(until) {
		delete(b.until, pool)
		return time.Time{}, false
	}
	return until, true
}

// Clear drops any hold on pool.
func (b *PoolBackoff) Clear(pool string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.until, pool)
}

// MarkFailedOp records that the broker reported operationID terminal FAILED with detail. The
// batch failure handler calls it for every FAILED add (the NodeClaim it deletes may already be
// terminating, and its Delete() must learn that no machine is coming) and for every remove the
// broker refused permanently (IsPermanentRefusalDetail), so Delete() stops re-sending it.
func (b *PoolBackoff) MarkFailedOp(operationID, detail string) {
	if b == nil || operationID == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	for id, f := range b.failed {
		if now.Sub(f.at) > failedOpRetention {
			delete(b.failed, id)
		}
	}
	b.failed[operationID] = failedOp{detail: detail, at: now}
}

// FailedOp reports whether operationID was recorded FAILED (within failedOpRetention) and the
// broker's detail.
func (b *PoolBackoff) FailedOp(operationID string) (detail string, ok bool) {
	if b == nil {
		return "", false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	f, ok := b.failed[operationID]
	if !ok {
		return "", false
	}
	if b.now().Sub(f.at) > failedOpRetention {
		delete(b.failed, operationID)
		return "", false
	}
	return f.detail, true
}

// markOfferingsUnavailable flags every offering of every instance type unavailable, in place. The
// scheduler only considers available offerings when deciding whether an instance type can host a
// pod, so this excludes the whole list from provisioning without touching the NodePool object.
func markOfferingsUnavailable(its []*cloudprovider.InstanceType) {
	for _, it := range its {
		for _, o := range it.Offerings {
			o.Available = false
		}
	}
}
