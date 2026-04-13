/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package rafaynodeclass

import (
	"context"

	"github.com/awslabs/operatorpkg/controller"
	"github.com/awslabs/operatorpkg/status"
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

// NewController returns a controller that marks each RafayNodeClass as Ready.
func NewController(kubeClient client.Client) *Controller {
	return &Controller{kubeClient: kubeClient}
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
	if !nc.StatusConditions().SetTrue(status.ConditionReady) {
		return reconcile.Result{}, nil
	}
	if err := c.kubeClient.Status().Patch(ctx, nc, client.MergeFrom(stored)); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}
