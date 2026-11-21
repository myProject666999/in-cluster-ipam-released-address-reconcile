/*
Copyright 2023 The Kubernetes Authors.

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

package controllers

import (
	"context"
	"net/netip"

	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	ipamv1 "sigs.k8s.io/cluster-api/api/ipam/v1beta2"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/api/v1alpha2"
	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/internal/poolutil"
	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/internal/reclaimer"
	pooltypes "sigs.k8s.io/cluster-api-ipam-provider-in-cluster/pkg/types"
)

const (
	inClusterIPPoolKind       = "InClusterIPPool"
	globalInClusterIPPoolKind = "GlobalInClusterIPPool"

	// ProtectPoolFinalizer is used to prevent deletion of a Pool object while its addresses have not been deleted.
	ProtectPoolFinalizer = "ipam.cluster.x-k8s.io/ProtectPool"
)

// InClusterIPPoolReconciler reconciles a InClusterIPPool object.
type InClusterIPPoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Reclaimer removes residual IPAddress objects whose claim no longer exists. It is
	// optional; when nil, orphan reclamation is skipped.
	Reclaimer *reclaimer.Reclaimer
}

// SetupWithManager sets up the controller with the Manager.
func (r *InClusterIPPoolReconciler) SetupWithManager(_ context.Context, mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha2.InClusterIPPool{}).
		Watches(
			&ipamv1.IPAddress{},
			handler.EnqueueRequestsFromMapFunc(r.ipAddressToInClusterIPPool)).
		Complete(r)
}

func (r *InClusterIPPoolReconciler) ipAddressToInClusterIPPool(_ context.Context, clientObj client.Object) []reconcile.Request {
	ipAddress, ok := clientObj.(*ipamv1.IPAddress)
	if !ok {
		return nil
	}

	if ipAddress.Spec.PoolRef.APIGroup == v1alpha2.GroupVersion.Group &&
		ipAddress.Spec.PoolRef.Kind == inClusterIPPoolKind {
		return []reconcile.Request{{
			NamespacedName: types.NamespacedName{
				Namespace: ipAddress.Namespace,
				Name:      ipAddress.Spec.PoolRef.Name,
			},
		}}
	}

	return nil
}

// GlobalInClusterIPPoolReconciler reconciles a GlobalInClusterIPPool object.
type GlobalInClusterIPPoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Reclaimer removes residual IPAddress objects whose claim no longer exists. It is
	// optional; when nil, orphan reclamation is skipped.
	Reclaimer *reclaimer.Reclaimer
}

// SetupWithManager sets up the controller with the Manager.
func (r *GlobalInClusterIPPoolReconciler) SetupWithManager(_ context.Context, mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha2.GlobalInClusterIPPool{}).
		Watches(
			&ipamv1.IPAddress{},
			handler.EnqueueRequestsFromMapFunc(r.ipAddressToGlobalInClusterIPPool)).
		Complete(r)
}

func (r *GlobalInClusterIPPoolReconciler) ipAddressToGlobalInClusterIPPool(_ context.Context, clientObj client.Object) []reconcile.Request {
	ipAddress, ok := clientObj.(*ipamv1.IPAddress)
	if !ok {
		return nil
	}

	if ipAddress.Spec.PoolRef.APIGroup == v1alpha2.GroupVersion.Group &&
		ipAddress.Spec.PoolRef.Kind == globalInClusterIPPoolKind {
		return []reconcile.Request{{
			NamespacedName: types.NamespacedName{
				Namespace: ipAddress.Namespace,
				Name:      ipAddress.Spec.PoolRef.Name,
			},
		}}
	}

	return nil
}

//+kubebuilder:rbac:groups=ipam.cluster.x-k8s.io,resources=inclusterippools,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=ipam.cluster.x-k8s.io,resources=inclusterippools/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=ipam.cluster.x-k8s.io,resources=inclusterippools/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *InClusterIPPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Reconciling pool")

	pool := &v1alpha2.InClusterIPPool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, errors.Wrap(err, "failed to fetch InClusterIPPool")
		}
		return ctrl.Result{}, nil
	}
	return genericReconcile(ctx, r.Client, pool, r.Reclaimer)
}

//+kubebuilder:rbac:groups=ipam.cluster.x-k8s.io,resources=globalinclusterippools,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=ipam.cluster.x-k8s.io,resources=globalinclusterippools/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=ipam.cluster.x-k8s.io,resources=globalinclusterippools/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *GlobalInClusterIPPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Reconciling pool")

	pool := &v1alpha2.GlobalInClusterIPPool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, errors.Wrap(err, "failed to fetch GlobalInClusterIPPool")
		}
		return ctrl.Result{}, nil
	}
	return genericReconcile(ctx, r.Client, pool, r.Reclaimer)
}

func genericReconcile(ctx context.Context, c client.Client, pool pooltypes.GenericInClusterPool, r *reclaimer.Reclaimer) (_ ctrl.Result, reterr error) {
	log := ctrl.LoggerFrom(ctx)

	patchHelper, err := patch.NewHelper(pool, c)
	if err != nil {
		return ctrl.Result{}, err
	}

	defer func() {
		if err := patchHelper.Patch(ctx, pool); err != nil {
			reterr = kerrors.NewAggregate([]error{reterr, err})
		}
	}()

	poolTypeRef := ipamv1.IPPoolReference{
		APIGroup: v1alpha2.GroupVersion.Group,
		Kind:     pool.GetObjectKind().GroupVersionKind().Kind,
		Name:     pool.GetName(),
	}

	if !controllerutil.ContainsFinalizer(pool, ProtectPoolFinalizer) {
		controllerutil.AddFinalizer(pool, ProtectPoolFinalizer)
	}

	poolDeleting := !pool.GetDeletionTimestamp().IsZero()

	// Reclaim residual addresses before the deletion short-circuit below, otherwise a
	// deleting pool with orphans could never drain. The reclaimer never mutates the pool;
	// it returns the retained objects and the desired requeue. Both branches below re-list
	// the addresses afterwards, so no list is needed here.
	var reclaimOutcome reclaimer.Outcome
	if r != nil {
		var reclaimErr error
		reclaimOutcome, reclaimErr = r.ReclaimOrphanedAddresses(ctx, pool.GetNamespace(), poolTypeRef, poolDeleting)
		if reclaimErr != nil {
			return ctrl.Result{}, errors.Wrap(reclaimErr, "failed to reclaim orphaned addresses")
		}
	}

	if poolDeleting {
		// Refresh the in-use count after deletions so the finalizer can be released as soon
		// as the pool is empty, within the same reconcile.
		addressesInUse, err := poolutil.ListAddressesInUse(ctx, c, pool.GetNamespace(), poolTypeRef)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to re-list addresses while pool is deleting")
		}
		if len(addressesInUse) == 0 {
			controllerutil.RemoveFinalizer(pool, ProtectPoolFinalizer)
		}
		return reclaimOutcome.Result, nil
	}

	poolIPSet, err := poolutil.PoolSpecToIPSet(pool.PoolSpec())
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to build ip set from pool spec")
	}

	poolCount := poolutil.IPSetCount(poolIPSet)
	if pool.PoolSpec().Gateway != "" {
		gatewayAddr, err := netip.ParseAddr(pool.PoolSpec().Gateway)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to parse pool gateway")
		}

		if poolIPSet.Contains(gatewayAddr) {
			poolCount--
		}
	}

	// Re-list after reclamation so capacity stats converge per batch rather than only
	// after an entire sweep.
	addressesInUse, err := poolutil.ListAddressesInUse(ctx, c, pool.GetNamespace(), poolTypeRef)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to re-list addresses")
	}
	inUseCount := len(addressesInUse)
	free := poolCount - inUseCount
	outOfRangeIPSet, err := poolutil.AddressesOutOfRangeIPSet(addressesInUse, poolIPSet)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to build out of range ip set")
	}

	pool.PoolStatus().Addresses = &v1alpha2.InClusterIPPoolStatusIPAddresses{
		Total:      poolCount,
		Used:       inUseCount,
		Free:       free,
		OutOfRange: poolutil.IPSetCount(outOfRangeIPSet),
	}
	pool.PoolStatus().OrphanedAddresses = orphanedStatusEntries(reclaimOutcome.Retained)
	pool.PoolStatus().OrphanedAddressCount = reclaimOutcome.RetainedTotal

	log.Info("Updating pool with usage info", "statusAddresses", pool.PoolStatus().Addresses)

	return reclaimOutcome.Result, nil
}

// orphanedStatusEntries maps the reclaimer's internal retained-object representation onto
// the v1alpha2 API status type.
func orphanedStatusEntries(retained []reclaimer.OrphanedAddress) []v1alpha2.OrphanedIPAddress {
	if len(retained) == 0 {
		return nil
	}
	entries := make([]v1alpha2.OrphanedIPAddress, 0, len(retained))
	for _, o := range retained {
		entries = append(entries, v1alpha2.OrphanedIPAddress{
			Name:      o.Name,
			Namespace: o.Namespace,
			Address:   o.Address,
			Reason:    string(o.Reason),
			Message:   o.Message,
		})
	}
	return entries
}
