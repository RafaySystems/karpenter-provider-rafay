/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cloudprovider

// p2_delete_retired_test.go — phase-2 regression tests for CloudProvider.Delete():
//
//   - R1-prov-cloudprovider-2: a machine the platform retired leaves a *Terminating* Node behind
//     (every registered Node carries Karpenter's termination finalizer), not a missing one; the
//     node termination controller then deletes the NodeClaim and calls Delete with that Node
//     present. Sending <uid>-remove for it retires a third machine, and so on down to the pool
//     floor.
//   - R3-karpenter-node-termination-get-1: today's broker reports a remove SUCCEEDED at catalog
//     publish, before the platform retired anything, and names no machine. Reporting NotFound at
//     once drops the Node object of a machine whose kubelet is alive. The kubelet heartbeat is
//     the signal: keep terminating while the Node is Ready (bounded by the settle window), report
//     gone once it is not, treat "still Ready after the window" as "retired another machine".
//   - The retired-other verdict is persisted as an annotation so a restarted provider (empty
//     memos, batcher with no result) never re-sends the remove.

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpcp "sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// p2Node is cpLiveNode with the Ready condition set to status.
func p2Node(providerID string, ready corev1.ConditionStatus) *corev1.Node {
	n := cpLiveNode(providerID)
	n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: ready}}
	return n
}

// p2Terminating marks node deleted at t under Karpenter's finalizer.
func p2Terminating(node *corev1.Node, t time.Time) *corev1.Node {
	ts := metav1.NewTime(t)
	node.DeletionTimestamp = &ts
	node.Finalizers = []string{karpv1.TerminationFinalizer}
	return node
}

// p2DeletingClaim is cpClaim with a real providerID and DeletionTimestamp t.
func p2DeletingClaim(t time.Time) *karpv1.NodeClaim {
	claim := cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID)
	ts := metav1.NewTime(t)
	claim.DeletionTimestamp = &ts
	return claim
}

// p2Provider is NewCloudProvider over cl with a capturing recorder and the fake batcher b.
func p2Provider(cl client.Client, b nodeBatcher) (*CloudProvider, *capturingRecorder) {
	rec := &capturingRecorder{}
	cp := NewCloudProvider(cl, cl, nil, cpClusterID, cpProjectID, nil, nil, WithEventRecorder(rec))
	cp.batcher = b
	return cp, rec
}

// ──────────────────────────── R1-prov-cloudprovider-2 ────────────────────────────

// TestP2DeleteSkipsRemoveForNodeRetiredExternally: Node deleted at T0 (before the NodeClaim, at
// T0+30s) with its kubelet stopped is what the platform's own retirement leaves behind. Delete
// must report the instance gone without a remove and say so on the NodePool.
func TestP2DeleteSkipsRemoveForNodeRetiredExternally(t *testing.T) {
	t0 := cpBase.Add(time.Hour)
	b := cpNewFakeBatcher()
	cl := cpFakeClient(t, p2Terminating(p2Node(cpRealID, corev1.ConditionFalse), t0), cpNodePool(cpPoolName))
	cp, rec := p2Provider(cl, b)

	err := cp.Delete(context.Background(), p2DeletingClaim(t0.Add(30*time.Second)))
	if !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete for an externally retired Node = %v, want NodeClaimNotFoundError", err)
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Fatalf("EnqueueRemove calls = %d, want 0 (a remove here retires another machine)", n)
	}
	if len(rec.events) != 1 || rec.events[0].Reason != NodeRetiredExternallyEventReason {
		t.Fatalf("events = %+v, want one %s", rec.events, NodeRetiredExternallyEventReason)
	}
	// Second call: same answer, one event.
	if err := cp.Delete(context.Background(), p2DeletingClaim(t0.Add(30*time.Second))); !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("second Delete = %v, want NodeClaimNotFoundError", err)
	}
	if len(rec.events) != 1 {
		t.Fatalf("events after the second call = %d, want still 1", len(rec.events))
	}
}

// TestP2DeleteSendsRemoveForHealthyNodeDeletedByOperator is the companion: same timestamps, but
// the kubelet is Ready (an operator deleted a healthy Node) — the machine is alive and the remove
// is sent exactly once.
func TestP2DeleteSendsRemoveForHealthyNodeDeletedByOperator(t *testing.T) {
	t0 := cpBase.Add(time.Hour)
	b := cpNewFakeBatcher()
	cl := cpFakeClient(t, p2Terminating(p2Node(cpRealID, corev1.ConditionTrue), t0))
	cp, _ := p2Provider(cl, b)

	if err := cp.Delete(context.Background(), p2DeletingClaim(t0.Add(30*time.Second))); err != nil {
		t.Fatalf("Delete for a Ready operator-deleted Node = %v, want nil after ACK", err)
	}
	if n := len(b.removeCalls()); n != 1 {
		t.Fatalf("EnqueueRemove calls = %d, want 1", n)
	}
}

// TestP2DeleteExternalRetireShape pins the remaining edges of retiredExternally: Karpenter's own
// path (Node deleted strictly after the NodeClaim) and a disruption-queue deletion send the
// remove even with a NotReady kubelet; the same-second case does not (deletion timestamps have
// second granularity, and the error that cannot cascade is "no remove").
func TestP2DeleteExternalRetireShape(t *testing.T) {
	t0 := cpBase.Add(time.Hour)
	cases := []struct {
		name       string
		nodeAt     time.Time
		claimAt    time.Time
		disrupted  bool
		wantRemove bool
	}{
		{name: "node deleted after the nodeclaim (Karpenter path)", nodeAt: t0.Add(2 * time.Second), claimAt: t0, wantRemove: true},
		{name: "same second", nodeAt: t0, claimAt: t0, wantRemove: false},
		{name: "chosen by the disruption queue", nodeAt: t0, claimAt: t0.Add(time.Second), disrupted: true, wantRemove: true},
		{name: "node deleted first", nodeAt: t0, claimAt: t0.Add(time.Second), wantRemove: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := cpNewFakeBatcher()
			cl := cpFakeClient(t, p2Terminating(p2Node(cpRealID, corev1.ConditionUnknown), tc.nodeAt))
			cp, _ := p2Provider(cl, b)
			claim := p2DeletingClaim(tc.claimAt)
			if tc.disrupted {
				claim.StatusConditions().SetTrueWithReason(karpv1.ConditionTypeDisruptionReason, "Underutilized", "Underutilized")
			}
			err := cp.Delete(context.Background(), claim)
			if got := len(b.removeCalls()); got != map[bool]int{true: 1, false: 0}[tc.wantRemove] {
				t.Fatalf("EnqueueRemove calls = %d, wantRemove=%v (err=%v)", got, tc.wantRemove, err)
			}
			if tc.wantRemove && err != nil {
				t.Fatalf("Delete = %v, want nil after ACK", err)
			}
			if !tc.wantRemove && !karpcp.IsNodeClaimNotFoundError(err) {
				t.Fatalf("Delete = %v, want NodeClaimNotFoundError", err)
			}
		})
	}
}

// ──────────────────────────── R3-karpenter-node-termination-get-1 ────────────────────────────

// TestP2DeleteUntargetedSucceededHoldsWhileNodeReady: SUCCEEDED with no provider IDs while this
// NodeClaim's Node is still Ready means the platform has not retired this machine (yet). Delete
// keeps the NodeClaim terminating (nil), sends nothing, and raises no event inside the window.
func TestP2DeleteUntargetedSucceededHoldsWhileNodeReady(t *testing.T) {
	b := cpNewFakeBatcher()
	b.markSucceeded(cpRemoveOp)
	cl := cpFakeClient(t, p2Node(cpRealID, corev1.ConditionTrue), cpNodePool(cpPoolName))
	cp, rec := p2Provider(cl, b)
	claim := cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID)

	for i := 0; i < 3; i++ {
		if err := cp.Delete(context.Background(), claim); err != nil {
			t.Fatalf("Delete #%d with a Ready Node = %v, want nil (the machine has not stopped)", i+1, err)
		}
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Fatalf("EnqueueRemove calls = %d, want 0", n)
	}
	if len(rec.events) != 0 {
		t.Fatalf("events = %+v, want none inside the settle window", rec.events)
	}
	if _, ok := cp.retiredOther.Load(cpRemoveOp); ok {
		t.Fatal("retired-other verdict recorded inside the settle window")
	}
}

// TestP2DeleteUntargetedSucceededConvergesOnceNodeNotReady: the kubelet stopped heartbeating —
// the platform retired this machine — so the instance is gone. A missing Ready condition counts
// as not Ready (TestDeleteSucceededShortCircuit covers the no-condition Node).
func TestP2DeleteUntargetedSucceededConvergesOnceNodeNotReady(t *testing.T) {
	for _, status := range []corev1.ConditionStatus{corev1.ConditionFalse, corev1.ConditionUnknown} {
		t.Run(string(status), func(t *testing.T) {
			b := cpNewFakeBatcher()
			b.markSucceeded(cpRemoveOp)
			cl := cpFakeClient(t, p2Node(cpRealID, status))
			cp, _ := p2Provider(cl, b)

			err := cp.Delete(context.Background(), cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID))
			if !karpcp.IsNodeClaimNotFoundError(err) {
				t.Fatalf("Delete with a NotReady Node = %v, want NodeClaimNotFoundError", err)
			}
			if n := len(b.removeCalls()); n != 0 {
				t.Fatalf("EnqueueRemove calls = %d, want 0", n)
			}
		})
	}
}

// TestP2DeleteUntargetedSucceededTransitionsReadyToGone: hold while Ready, converge as soon as
// the Node turns NotReady, then as soon as it disappears.
func TestP2DeleteUntargetedSucceededTransitionsReadyToGone(t *testing.T) {
	b := cpNewFakeBatcher()
	b.markSucceeded(cpRemoveOp)
	node := p2Node(cpRealID, corev1.ConditionTrue)
	cl := cpFakeClient(t, node)
	cp, _ := p2Provider(cl, b)
	claim := cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID)

	if err := cp.Delete(context.Background(), claim); err != nil {
		t.Fatalf("Delete while Ready = %v, want nil", err)
	}
	stored := &corev1.Node{}
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(node), stored); err != nil {
		t.Fatal(err)
	}
	stored.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionUnknown}}
	if err := cl.Status().Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if err := cp.Delete(context.Background(), claim); !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete once NotReady = %v, want NodeClaimNotFoundError", err)
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Fatalf("EnqueueRemove calls = %d, want 0", n)
	}
}

// TestP2DeleteUntargetedSucceededStillReadyAfterWindowIsRetiredOther: a Node still Ready once
// the settle window has elapsed means the platform retired another machine — same handling as a
// broker that named it: event, annotation on the stored NodeClaim, held (nil) while the Node
// exists, no re-send even after the batcher forgets the result, NotFound once the Node is gone.
func TestP2DeleteUntargetedSucceededStillReadyAfterWindowIsRetiredOther(t *testing.T) {
	b := cpNewFakeBatcher()
	b.markSucceeded(cpRemoveOp)
	node := p2Node(cpRealID, corev1.ConditionTrue)
	claim := cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID)
	cl := cpFakeClient(t, node, claim, cpNodePool(cpPoolName))
	cp, rec := p2Provider(cl, b)
	// The SUCCEEDED was first seen longer than the window ago.
	cp.succeededSeen.Store(cpRemoveOp, time.Now().Add(-cp.removeSettleWindow()-time.Minute))

	for i := 0; i < 2; i++ {
		if err := cp.Delete(context.Background(), claim); err != nil {
			t.Fatalf("Delete #%d after the window = %v, want nil while the Node exists", i+1, err)
		}
	}
	if len(rec.events) != 1 || rec.events[0].Reason != RemoveRetiredOtherMachineEventReason || !strings.Contains(rec.events[0].Message, "still Ready") {
		t.Fatalf("events = %+v, want one %s saying the machine is still Ready", rec.events, RemoveRetiredOtherMachineEventReason)
	}
	stored := &karpv1.NodeClaim{}
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(claim), stored); err != nil {
		t.Fatal(err)
	}
	if got := stored.Annotations[RemoveRetiredOtherAnnotationKey]; got != retiredOtherUnknown {
		t.Fatalf("annotation %s = %q, want %q", RemoveRetiredOtherAnnotationKey, got, retiredOtherUnknown)
	}
	// The batcher forgets the result: still held, never re-sent.
	b.succeeded = map[string]bool{}
	if err := cp.Delete(context.Background(), claim); err != nil {
		t.Fatalf("Delete after the batcher forgot = %v, want nil", err)
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Fatalf("EnqueueRemove calls = %d, want 0 (a re-send retires a third machine)", n)
	}
	if err := cl.Delete(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	if err := cp.Delete(context.Background(), claim); !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete with no Node carrying the ID = %v, want NodeClaimNotFoundError", err)
	}
}

// TestP2DeleteSettleWindowNeverShorterThanDefault: the option clamps.
func TestP2DeleteSettleWindowNeverShorterThanDefault(t *testing.T) {
	cp := NewCloudProvider(nil, nil, nil, cpClusterID, cpProjectID, nil, nil, WithRemoveSettleWindow(5*time.Minute))
	if got := cp.removeSettleWindow(); got != DefaultRemoveSettleWindow {
		t.Fatalf("settle window = %s, want the %s floor", got, DefaultRemoveSettleWindow)
	}
	cp = NewCloudProvider(nil, nil, nil, cpClusterID, cpProjectID, nil, nil, WithRemoveSettleWindow(2*time.Hour))
	if got := cp.removeSettleWindow(); got != 2*time.Hour {
		t.Fatalf("settle window = %s, want 2h", got)
	}
	if got := (&CloudProvider{}).removeSettleWindow(); got != DefaultRemoveSettleWindow {
		t.Fatalf("unset settle window = %s, want %s", got, DefaultRemoveSettleWindow)
	}
}

// ──────────────────────────── persisted retired-other verdict ────────────────────────────

// TestP2DeleteHonoursRetiredOtherAnnotationAfterRestart: a fresh CloudProvider (empty memos, a
// batcher with no result for the op) over a NodeClaim carrying the annotation must not send the
// remove while a Node carries the ID, and converges once none does.
func TestP2DeleteHonoursRetiredOtherAnnotationAfterRestart(t *testing.T) {
	b := cpNewFakeBatcher()
	node := p2Node(cpRealID, corev1.ConditionTrue)
	cl := cpFakeClient(t, node)
	cp, rec := p2Provider(cl, b)
	claim := cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID)
	claim.Annotations = map[string]string{RemoveRetiredOtherAnnotationKey: fupOtherMachineID}

	for i := 0; i < 2; i++ {
		if err := cp.Delete(context.Background(), claim); err != nil {
			t.Fatalf("Delete #%d with the annotation = %v, want nil while the Node exists", i+1, err)
		}
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Fatalf("EnqueueRemove calls = %d, want 0 (the verdict on the object outlives the process)", n)
	}
	if len(rec.events) != 0 {
		t.Fatalf("events = %+v, want none (already raised before the restart)", rec.events)
	}
	if err := cl.Delete(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	if err := cp.Delete(context.Background(), claim); !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete with no Node carrying the ID = %v, want NodeClaimNotFoundError", err)
	}
}

// TestP2DeleteBrokerNamedOtherMachinePersistsAnnotation: the broker-named variant writes the
// named IDs into the annotation on the stored NodeClaim.
func TestP2DeleteBrokerNamedOtherMachinePersistsAnnotation(t *testing.T) {
	b := fupNewBatcher()
	b.markRetired(cpRemoveOp, fupOtherMachineID)
	claim := cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID)
	cl := cpFakeClient(t, cpLiveNode(cpRealID), claim)
	cp, _ := p2Provider(cl, b)

	if err := cp.Delete(context.Background(), claim); err != nil {
		t.Fatalf("Delete = %v, want nil", err)
	}
	stored := &karpv1.NodeClaim{}
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(claim), stored); err != nil {
		t.Fatal(err)
	}
	if got := stored.Annotations[RemoveRetiredOtherAnnotationKey]; got != fupOtherMachineID {
		t.Fatalf("annotation = %q, want %q", got, fupOtherMachineID)
	}
}
