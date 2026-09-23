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

// Package batchresume hands the status poller back the add batches a previous incarnation of this
// provider sent before it restarted.
//
// The NodeBatcher tracks sent batches only in memory. A restart forgets them, and for adds nothing
// ever re-sends: once the broker ACKs, the NodeClaim is Launched and Karpenter's launch reconciler
// will not call Create() again. The node usually still arrives and NodeProviderIDController binds
// it, so success needs no help — but a FAILED add would go unnoticed, and its pending NodeClaim
// would sit until Karpenter's registration timeout (60 min) reaped it, instead of being deleted
// within a couple of minutes by the batcher's failure handler.
//
// Create() stamps each NodeClaim with the batch it was sent in (cloudprovider.BatchIDAnnotationKey).
// On startup this controller lists NodeClaims that are still pending, groups them by that
// annotation, and re-registers each batch with the batcher, which then polls it as if it had sent
// it. A batch the broker no longer knows (its 90-minute index expired) produces empty polls, and
// the batcher treats those exactly as it always has: the items are reported failed and the
// pending NodeClaims are reaped — which is the right outcome, because that add really is lost.
//
// Removes are deliberately not resumed: Karpenter re-issues Delete() on a loop with the same
// deterministic operation ID, so they recover through the normal path within seconds.
package batchresume

import (
	"context"
	"strings"

	"github.com/awslabs/operatorpkg/controller"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	cprovider "github.com/RafaySystems/karpenter-provider-rafay/pkg/cloudprovider"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/rafay"
)

// Controller runs once on startup and resumes polling for pending add batches.
type Controller struct {
	// apiReader reads straight from the API server. The informer cache is not guaranteed warm
	// at the moment this runs, and a partial list here would silently leave batches untracked.
	apiReader client.Reader
	batcher   *rafay.NodeBatcher
}

var _ controller.Controller = (*Controller)(nil)

// NewController returns a controller that resumes the batches recorded on pending NodeClaims.
func NewController(apiReader client.Reader, batcher *rafay.NodeBatcher) *Controller {
	return &Controller{apiReader: apiReader, batcher: batcher}
}

// Register adds the one-shot resume as a manager Runnable. It is a Runnable rather than a
// Reconciler because the work is a single pass over existing objects at startup, not a response
// to changes.
func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return m.Add(c)
}

// NeedLeaderElection ties the resume to the replica whose batcher actually sends and polls;
// a standby's batcher is idle and must not start polling batches it does not own.
func (c *Controller) NeedLeaderElection() bool { return true }

// Start performs the single resume pass and returns. Errors are logged, never returned: failing
// here would stop the whole manager, and the cost of a missed resume is only the slower,
// registration-timeout-based recovery that existed before this controller.
func (c *Controller) Start(ctx context.Context) error {
	var claims karpv1.NodeClaimList
	if err := c.apiReader.List(ctx, &claims); err != nil {
		klog.Errorf("batchresume: listing NodeClaims failed; pending add batches will not be resumed: %v", err)
		return nil
	}

	byBatch := map[string][]string{}
	for i := range claims.Items {
		nc := &claims.Items[i]
		if !nc.DeletionTimestamp.IsZero() {
			continue
		}
		// Only a NodeClaim still waiting for its node has anything left to poll for.
		if !strings.HasPrefix(nc.Status.ProviderID, cprovider.PendingProviderIDPrefix) {
			continue
		}
		batchID := strings.TrimSpace(nc.Annotations[cprovider.BatchIDAnnotationKey])
		if batchID == "" {
			// Created by a provider from before the annotation existed; it falls back to the
			// registration-timeout path as it always did.
			continue
		}
		byBatch[batchID] = append(byBatch[batchID], string(nc.UID))
	}

	resumed, claimsResumed := 0, 0
	for batchID, ops := range byBatch {
		if c.batcher.ResumeAddBatch(batchID, ops) {
			resumed++
			claimsResumed += len(ops)
		}
	}
	klog.Infof("batchresume: resumed %d add batch(es) covering %d pending NodeClaim(s)", resumed, claimsResumed)
	return nil
}
