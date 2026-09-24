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

// IsPoolAtMaxDetail reports whether a FAILED add operation's detail says the pool is at its
// platform maximum, i.e. the failure is not transient and reprovisioning right away would only
// be refused again.
func IsPoolAtMaxDetail(detail string) bool {
	d := strings.ToLower(detail)
	for _, m := range poolAtMaxDetailMarkers {
		if strings.Contains(d, m) {
			return true
		}
	}
	return false
}

// PoolBackoff remembers, per NodePool, until when the broker's "pool at maximum" refusal should
// keep Karpenter from provisioning into that pool. The batch failure handler marks a pool when the
// broker reports the refusal; GetInstanceTypes renders the pool's offerings unavailable while the
// mark is active, which is the standard Karpenter way to tell the scheduler "not here, not now"
// (the pods stay pending with a "no instance type has the required offering" message, and no
// NodeClaim is created). Entries expire on their own; nothing has to clear them.
//
// A nil *PoolBackoff is a valid, inert instance: Mark is a no-op and Until never reports active.
type PoolBackoff struct {
	mu       sync.Mutex
	cooldown time.Duration
	until    map[string]time.Time
	now      func() time.Time
}

// NewPoolBackoff returns a PoolBackoff holding each marked pool for cooldown. A non-positive
// cooldown disables the backoff (Mark becomes a no-op), which is the escape hatch for a
// deployment that wants the old delete-and-reprovision-immediately behaviour.
func NewPoolBackoff(cooldown time.Duration) *PoolBackoff {
	return &PoolBackoff{cooldown: cooldown, until: map[string]time.Time{}, now: time.Now}
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
