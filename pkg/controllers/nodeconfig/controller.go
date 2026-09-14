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

// Package nodeconfig bootstraps the cluster's Karpenter objects from edge-broker.
//
// The Rafay platform already knows which worker node pools a cluster has and what each node
// SKU looks like — it is the same "Worker Node Pool" catalog that scale-out and scale-in
// mutate. Before this controller existed an operator had to hand-write a matching
// RafayNodeClass + NodePool per cluster and keep them in sync by hand; a pool renamed in the
// catalog, or a SKU whose CPU/memory changed, silently produced NodeClaims the platform could
// not satisfy.
//
// So on startup the controller asks the broker for that config and applies it, then re-syncs
// on an interval to pick up pools added or resized in the catalog afterwards.
//
// Deliberately NOT done here: pruning. A pool that disappears from the broker's response is
// left alone and only logged. The broker drops a pool from its response whenever a node SKU
// lookup fails (see its Warnings field), so a transient PaaS error would otherwise delete a
// live NodePool — and deleting a NodePool drains and terminates every node under it. Removing
// a pool for real stays an explicit operator action.
package nodeconfig

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/awslabs/operatorpkg/controller"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	edgev1 "github.com/RafaySystems/edge-common/pkg/edge/v1"
)

const (
	// fieldOwner identifies this controller in server-side apply managedFields.
	fieldOwner = client.FieldOwner("karpenter-provider-rafay")

	// managedByLabel marks every object this controller applies, so `kubectl get nodepools -l`
	// can tell broker-managed pools from hand-written ones.
	managedByLabel = "karpenter.rafay.io/managed-by"
	managedByValue = "edge-broker"

	// revisionAnnotation records the broker revision an object was last applied from. It is
	// informational: the skip decision uses the in-memory lastRevision, and this makes the
	// same fact visible with kubectl.
	revisionAnnotation = "karpenter.rafay.io/config-revision"

	// defaultSyncInterval is how often the config is re-fetched after the first successful
	// apply. Node pools change at human speed (an operator edits the catalog), so this only
	// needs to be fast enough that a catalog edit takes effect without a controller restart.
	defaultSyncInterval = 10 * time.Minute

	// Initial-fetch retry backoff. The broker connection often is not ready at the instant the
	// controller starts (certs mounting, broker rolling), so the first fetch retries patiently
	// instead of leaving the cluster with no NodePools.
	initialRetryDelay = 5 * time.Second
	maxRetryDelay     = 2 * time.Minute
)

// ConfigFetcher is the broker call this controller needs (rafay.BrokerClient implements it).
type ConfigFetcher interface {
	GetKarpenterConfig(ctx context.Context, clusterID, projectID string) (*edgev1.KarpenterConfigResponse, error)
}

// Controller fetches the cluster's Karpenter config from edge-broker and applies it.
type Controller struct {
	fetcher      ConfigFetcher
	clusterID    string
	projectID    string
	syncInterval time.Duration

	kubeClient client.Client
	// lastRevision is the broker revision of the last successful apply; a resync returning the
	// same revision is skipped. Only the leader runs the sync loop, so this needs no locking.
	lastRevision string
}

var _ controller.Controller = (*Controller)(nil)

// NewController returns a controller that keeps the cluster's RafayNodeClass and NodePool
// objects in sync with edge-broker.
func NewController(fetcher ConfigFetcher, clusterID, projectID string, syncInterval time.Duration) *Controller {
	if syncInterval <= 0 {
		syncInterval = defaultSyncInterval
	}
	return &Controller{
		fetcher:      fetcher,
		clusterID:    clusterID,
		projectID:    projectID,
		syncInterval: syncInterval,
	}
}

// SyncIntervalFromEnv reads KARPENTER_CONFIG_SYNC_INTERVAL (a Go duration, e.g. "5m").
// An unset or unparseable value keeps the default.
func SyncIntervalFromEnv() time.Duration {
	raw := strings.TrimSpace(os.Getenv("KARPENTER_CONFIG_SYNC_INTERVAL"))
	if raw == "" {
		return defaultSyncInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		klog.Warningf("nodeconfig: invalid KARPENTER_CONFIG_SYNC_INTERVAL=%q, using %s", raw, defaultSyncInterval)
		return defaultSyncInterval
	}
	return d
}

// EnabledFromEnv reports whether broker-driven bootstrap should run. It defaults to on;
// KARPENTER_CONFIG_BOOTSTRAP=false is the escape hatch for a cluster whose NodePools are
// managed by hand or by GitOps, where an apply from the broker would fight the real owner.
func EnabledFromEnv() bool {
	raw := strings.TrimSpace(os.Getenv("KARPENTER_CONFIG_BOOTSTRAP"))
	if raw == "" {
		return true
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		klog.Warningf("nodeconfig: invalid KARPENTER_CONFIG_BOOTSTRAP=%q, defaulting to enabled", raw)
		return true
	}
	return enabled
}

// Register adds the sync loop to the manager. It is a Runnable rather than a Reconciler
// because there is no in-cluster object to watch: the trigger is the broker's state, which the
// controller can only discover by asking.
func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	c.kubeClient = m.GetClient()
	return m.Add(c)
}

// NeedLeaderElection keeps the applies to a single replica. Server-side apply makes concurrent
// writers safe, but a single writer keeps managedFields and the logs readable.
func (c *Controller) NeedLeaderElection() bool { return true }

// Start runs the initial fetch-and-apply (retrying until it succeeds or ctx ends) and then
// re-syncs on the configured interval.
//
// A failure never aborts the controller: returning an error here would tear down the whole
// manager, taking provisioning down with it. Karpenter can still serve any NodePools already
// in the cluster while the broker is unreachable, so the loop just keeps retrying.
func (c *Controller) Start(ctx context.Context) error {
	klog.Infof("nodeconfig: syncing Karpenter config from edge-broker every %s", c.syncInterval)

	delay := initialRetryDelay
	for {
		if err := c.sync(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			klog.Errorf("nodeconfig: sync failed, retrying in %s: %v", delay, err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(delay):
			}
			if delay *= 2; delay > maxRetryDelay {
				delay = maxRetryDelay
			}
			continue
		}
		delay = initialRetryDelay
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(c.syncInterval):
		}
	}
}

// sync fetches the config once and applies it.
func (c *Controller) sync(ctx context.Context) error {
	resp, err := c.fetcher.GetKarpenterConfig(ctx, c.clusterID, c.projectID)
	if err != nil {
		// NotFound is the broker saying "this cluster has no Karpenter config to serve" — no
		// workspace compute instance behind the edge, or no worker-pool catalog on it. That is
		// a property of the cluster, not a transient fault: error-backoff would just repeat the
		// same answer every few seconds forever. Treat it as a successful no-op sync so the
		// loop re-checks at the normal interval (the catalog can appear later, e.g. when the
		// compute instance is created or autoscaling is enabled on a pool).
		if status.Code(err) == codes.NotFound {
			klog.Warningf("nodeconfig: edge-broker has no Karpenter config for this cluster; "+
				"nothing to apply, checking again in %s (%v)", c.syncInterval, err)
			return nil
		}
		return fmt.Errorf("fetch config from edge-broker: %w", err)
	}
	if resp == nil {
		return fmt.Errorf("edge-broker returned an empty config response")
	}

	// Warnings mean the broker dropped a pool it could not render. Surface them every sync:
	// they are the only signal an operator gets that a pool is missing from the cluster.
	for _, w := range resp.GetWarnings() {
		klog.Warningf("nodeconfig: edge-broker warning for cluster %q: %s", resp.GetClusterName(), w)
	}

	if !resp.GetAutoScaling() {
		// Applying NodePools would start autoscaling a cluster whose owner turned it off.
		// Objects already applied are left in place — removing them would drain nodes, and the
		// operator turning the toggle back on is the cheap, reversible path.
		klog.Infof("nodeconfig: autoscaling is disabled on compute instance %q; not applying Karpenter config", resp.GetClusterName())
		return nil
	}

	revision := strings.TrimSpace(resp.GetRevision())
	if revision != "" && revision == c.lastRevision {
		klog.V(2).Infof("nodeconfig: config revision %s unchanged, nothing to apply", revision)
		return nil
	}

	// Node classes go first: a NodePool whose nodeClassRef does not resolve stays NotReady
	// until the class shows up, and applying in this order avoids that window entirely.
	classes, err := decodeManifests(resp.GetNodeClassYaml())
	if err != nil {
		return fmt.Errorf("decode RafayNodeClass manifests: %w", err)
	}
	pools, err := decodeManifests(resp.GetNodePoolYaml())
	if err != nil {
		return fmt.Errorf("decode NodePool manifests: %w", err)
	}
	if len(classes) == 0 && len(pools) == 0 {
		klog.Warningf("nodeconfig: edge-broker returned no node classes or node pools for cluster %q; leaving the cluster untouched", resp.GetClusterName())
		return nil
	}

	for _, obj := range append(classes, pools...) {
		if err := c.apply(ctx, obj, revision); err != nil {
			return fmt.Errorf("apply %s %q: %w", obj.GetKind(), obj.GetName(), err)
		}
	}

	c.lastRevision = revision
	klog.Infof("nodeconfig: applied %d RafayNodeClass and %d NodePool objects for cluster %q (revision %s)",
		len(classes), len(pools), resp.GetClusterName(), revision)
	c.logOrphans(ctx, classes, pools)
	return nil
}

// apply server-side-applies one object.
//
// ForceOwnership is intentional: without it, any field a previous owner (a hand-run `kubectl
// apply`, an older field manager) still claims makes the apply fail with a conflict, and the
// controller would then retry forever instead of converging. The broker is the source of truth
// for these objects — set KARPENTER_CONFIG_BOOTSTRAP=false on clusters where it should not be.
func (c *Controller) apply(ctx context.Context, obj *unstructured.Unstructured, revision string) error {
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[managedByLabel] = managedByValue
	obj.SetLabels(labels)

	if revision != "" {
		annotations := obj.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[revisionAnnotation] = revision
		obj.SetAnnotations(annotations)
	}

	if err := c.kubeClient.Patch(ctx, obj, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
		return err
	}
	klog.V(2).Infof("nodeconfig: applied %s/%s", obj.GetKind(), obj.GetName())
	return nil
}

// logOrphans reports objects this controller owns that are no longer in the broker's config.
// They are never deleted here (see the package comment); naming them is what lets an operator
// decide whether the pool really is gone and remove it deliberately.
func (c *Controller) logOrphans(ctx context.Context, classes, pools []*unstructured.Unstructured) {
	report := func(listKind string, apiVersion string, applied []*unstructured.Unstructured) {
		wanted := make(map[string]bool, len(applied))
		for _, o := range applied {
			wanted[o.GetName()] = true
		}
		var list unstructured.UnstructuredList
		list.SetAPIVersion(apiVersion)
		list.SetKind(listKind)
		if err := c.kubeClient.List(ctx, &list, client.MatchingLabels{managedByLabel: managedByValue}); err != nil {
			if !apierrors.IsNotFound(err) {
				klog.V(2).Infof("nodeconfig: could not list %s to check for orphans: %v", listKind, err)
			}
			return
		}
		for i := range list.Items {
			if name := list.Items[i].GetName(); !wanted[name] {
				klog.Warningf("nodeconfig: %s %q is managed by edge-broker but is no longer in its config "+
					"(the pool may have been removed from the catalog, or its node SKU could not be read). "+
					"It is left in place; delete it manually once you are sure the pool is gone",
					list.Items[i].GetKind(), name)
			}
		}
	}
	report("RafayNodeClassList", "karpenter.rafay.io/v1alpha1", classes)
	report("NodePoolList", "karpenter.sh/v1", pools)
}

// decodeManifests splits a multi-document YAML string into unstructured objects. Empty
// documents (a trailing "---", a comment-only block) are skipped rather than rejected.
func decodeManifests(manifest string) ([]*unstructured.Unstructured, error) {
	if strings.TrimSpace(manifest) == "" {
		return nil, nil
	}
	var out []*unstructured.Unstructured
	dec := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(manifest), 4096)
	for {
		obj := &unstructured.Unstructured{}
		err := dec.Decode(obj)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(obj.Object) == 0 {
			continue
		}
		if obj.GetKind() == "" || obj.GetAPIVersion() == "" {
			return nil, fmt.Errorf("manifest document is missing apiVersion or kind")
		}
		if obj.GetName() == "" {
			return nil, fmt.Errorf("%s manifest is missing metadata.name", obj.GetKind())
		}
		out = append(out, obj)
	}
	return out, nil
}
