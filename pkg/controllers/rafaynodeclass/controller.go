/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package rafaynodeclass

import (
	"context"
	"fmt"

	"github.com/awslabs/operatorpkg/controller"
	"github.com/awslabs/operatorpkg/status"
	"k8s.io/apimachinery/pkg/api/resource"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/apis/v1alpha1"
)

// Controller sets status.conditions[Ready] on RafayNodeClass so Karpenter's nodepool.readiness
// can mark NodePools ready (see sigs.k8s.io/karpenter/pkg/controllers/nodepool/readiness).
type Controller struct {
	kubeClient client.Client
}

var _ controller.Controller = (*Controller)(nil)

// NewController returns a controller that marks each RafayNodeClass Ready after validating
// its instance types (or NotReady with reason ValidationFailed).
func NewController(kubeClient client.Client) *Controller {
	return &Controller{kubeClient: kubeClient}
}

// validateNodeClass checks that the NodeClass declares at least one instance type and that
// every entry has a non-empty name and parseable cpu/memory (and, while it exists, nvidia.com/gpu)
// quantities. Returns an empty string when valid, or a message naming the first offending entry.
func validateNodeClass(nc *v1alpha1.RafayNodeClass) string {
	if len(nc.Spec.InstanceTypes) == 0 {
		return "spec.instanceTypes must contain at least one entry"
	}
	for i, it := range nc.Spec.InstanceTypes {
		if it.Name == "" {
			return fmt.Sprintf("spec.instanceTypes[%d] has an empty name", i)
		}
		if _, err := resource.ParseQuantity(it.CPU); err != nil {
			return fmt.Sprintf("instance type %q has invalid cpu %q: %v", it.Name, it.CPU, err)
		}
		if _, err := resource.ParseQuantity(it.Memory); err != nil {
			return fmt.Sprintf("instance type %q has invalid memory %q: %v", it.Name, it.Memory, err)
		}
		// TEMPORARY — delete with v1alpha1.InstanceTypeSpec.GPU. Checked here for the same reason
		// as cpu/memory: rafayInstanceTypesToKarpenter rejects an unparseable quantity, so a
		// NodeClass that passed readiness with one would report Ready=True and then fail every
		// provisioning attempt instead of saying why.
		if it.GPU != "" {
			if _, err := resource.ParseQuantity(it.GPU); err != nil {
				return fmt.Sprintf("instance type %q has invalid nvidia.com/gpu %q: %v", it.Name, it.GPU, err)
			}
		}
	}
	return ""
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		For(&v1alpha1.RafayNodeClass{}).
		WithOptions(crcontroller.Options{MaxConcurrentReconciles: 10}).
		Named("rafaynodeclass.readiness").
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}

func (c *Controller) Reconcile(ctx context.Context, nc *v1alpha1.RafayNodeClass) (reconcile.Result, error) {
	if nc.GetDeletionTimestamp() != nil {
		return reconcile.Result{}, nil
	}
	stored := nc.DeepCopy()
	var changed bool
	if msg := validateNodeClass(nc); msg != "" {
		changed = nc.StatusConditions().SetFalse(status.ConditionReady, "ValidationFailed", msg)
	} else {
		changed = nc.StatusConditions().SetTrue(status.ConditionReady)
	}
	if !changed {
		return reconcile.Result{}, nil
	}
	if err := c.kubeClient.Status().Patch(ctx, nc, client.MergeFrom(stored)); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}
