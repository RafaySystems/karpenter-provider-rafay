/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package nodeconfig

// fup_notfound_test.go — regression tests for R3-broker-error-taxonomy-xrepo-2: sync used to
// treat every gRPC NotFound as "this cluster has no Karpenter config" without looking for the
// edgev1.ErrKarpenterConfigUnavailable marker the broker attaches only to the genuine
// empty-source case. A resolver blip at startup (edgesrv briefly Unavailable, surfaced by an
// older broker as a marker-less NotFound) therefore left the cluster without NodePools for a
// full sync interval instead of being retried on the 5s..2m backoff. Only the marker-carrying
// NotFound is a quiet no-op now (isConfigUnavailable).

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	edgev1 "github.com/RafaySystems/edge-common/pkg/edge/v1"
)

// fupResolverNotFound is the broker's shape for a transient resolver failure
// (karpenter_config.go: `status.Errorf(codes.NotFound, "karpenter config: %v", err)`).
func fupResolverNotFound() error {
	return status.Error(codes.NotFound, "karpenter config: resolve edge: get edge: rpc error: code = Unavailable desc = edgesrv down")
}

// fupUnavailableNotFound is the broker's shape for a real missing source, carrying the marker.
func fupUnavailableNotFound() error {
	return status.Errorf(codes.NotFound, "karpenter config: %v: no workspace compute instance named %q", edgev1.ErrKarpenterConfigUnavailable, "k8s-autoscale-01")
}

// fupCountingFetcher scripts one error per call and cancels the controller's context on the
// call after the last scripted one, so Start can be driven through its retry path.
type fupCountingFetcher struct {
	errs   []error
	cancel context.CancelFunc
	calls  int
	at     []time.Time
}

func (f *fupCountingFetcher) GetKarpenterConfig(context.Context, string, string) (*edgev1.KarpenterConfigResponse, error) {
	f.at = append(f.at, time.Now())
	f.calls++
	if f.calls <= len(f.errs) {
		return nil, f.errs[f.calls-1]
	}
	f.cancel()
	return nil, status.Error(codes.Canceled, "test: controller stopped")
}

// TestFupSyncNotFoundWithoutUnavailableMarkerIsRetried: a NotFound that does not carry the
// config-unavailable marker is a resolver failure, not "no config": sync must return an error
// so Start applies its retry backoff, and must not record anything.
func TestFupSyncNotFoundWithoutUnavailableMarkerIsRetried(t *testing.T) {
	fetcher := &stubFetcher{err: fupResolverNotFound()}
	c, _ := newTestController(t, fetcher)

	err := c.sync(context.Background())
	if err == nil {
		t.Fatal("sync on a marker-less NotFound = nil: a resolver blip is treated as 'cluster has no config' and the cluster stays without NodePools for a full sync interval")
	}
	if c.lastRevision != "" {
		t.Errorf("lastRevision must stay empty after a failed fetch, got %q", c.lastRevision)
	}
	if fetcher.calls != 1 {
		t.Errorf("fetch calls = %d, want 1", fetcher.calls)
	}
}

// TestFupSyncNotFoundWithUnavailableMarkerIsNoOp is the companion: the marker means the
// broker genuinely has no source for this cluster, which is a quiet no-op until the next
// interval.
func TestFupSyncNotFoundWithUnavailableMarkerIsNoOp(t *testing.T) {
	fetcher := &stubFetcher{err: fupUnavailableNotFound()}
	c, _ := newTestController(t, fetcher)

	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("sync on a marker-carrying NotFound = %v, want nil (quiet no-op)", err)
	}
	if c.lastRevision != "" {
		t.Errorf("a no-op sync must not record a revision, got %q", c.lastRevision)
	}
}

// TestFupStartRetriesMarkerlessNotFoundOnBackoff: driven through Start, a marker-less NotFound
// must be re-fetched after initialRetryDelay (5s), not after the sync interval (1h here).
// Bounded at twice the retry delay so a regression fails fast instead of hanging.
func TestFupStartRetriesMarkerlessNotFoundOnBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetcher := &fupCountingFetcher{errs: []error{fupResolverNotFound()}, cancel: cancel}
	c := NewController(fetcher, "cluster-1", "project-1", time.Hour)
	c.kubeClient = nil // never reached: every fetch errors

	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start = %v, want nil once its context is cancelled", err)
		}
	case <-time.After(2 * initialRetryDelay):
		cancel()
		t.Fatalf("Start did not re-fetch within %s of a marker-less NotFound: it is waiting out the %s sync interval", 2*initialRetryDelay, c.syncInterval)
	}
	if fetcher.calls != 2 {
		t.Fatalf("fetch calls = %d, want 2 (the failed fetch and its retry)", fetcher.calls)
	}
	if gap := fetcher.at[1].Sub(fetcher.at[0]); gap < initialRetryDelay || gap > 2*initialRetryDelay {
		t.Errorf("retry gap = %s, want ~%s (initialRetryDelay)", gap, initialRetryDelay)
	}
}
