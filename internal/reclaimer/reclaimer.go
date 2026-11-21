/*
Copyright The Kubernetes Authors.

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

package reclaimer

import (
	"context"
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ipamv1 "sigs.k8s.io/cluster-api/api/ipam/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	providerannotations "sigs.k8s.io/cluster-api-ipam-provider-in-cluster/pkg/annotations"
)

// confirmAndDelete runs the second (non-cached) confirmation for each candidate and
// deletes only those still proven residual. It mutates the outcome in place and stops the
// pass early on a transient confirmation error (Abandon) or when the per-pass cap is hit
// (BatchDeferred).
func (r *Reclaimer) confirmAndDelete(ctx context.Context, outcome *Outcome, candidates []ipamv1.IPAddress) {
	log := ctrl.LoggerFrom(ctx)
	direct := r.direct()

	for i := range candidates {
		if i >= maxCandidatesPerPass {
			outcome.Action = BatchDeferred
			outcome.Result = ctrl.Result{RequeueAfter: batchRequeue}
			log.Info("Reclaim pass reached per-passthrough cap; remaining candidates deferred",
				"deferred", len(candidates)-maxCandidatesPerPass, "cap", maxCandidatesPerPass)
			return
		}

		candidate := candidates[i]
		key := client.ObjectKeyFromObject(&candidate)

		confirmed, retain, abandon := r.confirmCandidate(ctx, direct, &candidate)
		if abandon != nil {
			outcome.Action = Abandon
			outcome.Result = ctrl.Result{RequeueAfter: abandonRequeue}
			outcome.addRetained(candidate, ConfirmationFailed, abandon.Error())
			log.Info("Abandoning reclaim pass after failed confirmation; no further deletions this round",
				"ipAddress", key.String(), "error", abandon.Error())
			return
		}
		if retain != nil {
			outcome.addRetained(candidate, retain.reason, retain.message)
			log.Info("Retaining IPAddress after direct confirmation",
				"ipAddress", key.String(), "reason", retain.reason, "message", retain.message)
			continue
		}
		if !confirmed {
			// Cache lag: the direct read showed a live matching claim. Nothing to delete.
			continue
		}

		if err := r.deleteConfirmed(ctx, &candidate); err != nil {
			outcome.Action = Abandon
			outcome.Result = ctrl.Result{RequeueAfter: abandonRequeue}
			outcome.addRetained(candidate, ConfirmationFailed, err.Error())
			log.Info("Abandoning reclaim pass after deletion failure",
				"ipAddress", key.String(), "error", err.Error())
			return
		}

		outcome.Deleted = append(outcome.Deleted, key)
		// Any prior pass state is Noop at this point; BatchDeferred and Abandon are set by
		// their own early returns. Promote to Deleted for the default (no requeue) result.
		if outcome.Action == Noop {
			outcome.Action = Deleted
		}
		log.Info("Deleted residual IPAddress after direct confirmation",
			"ipAddress", key.String(),
			"address", candidate.Spec.Address,
			"claim", candidate.Spec.ClaimRef.Name,
			"claimUID", candidate.Annotations[providerannotations.ClaimUIDAnnotation])
	}
}

// confirmCandidate performs the non-cached second read.
//
//	confirmed=true  -> residual proven, safe to delete
//	retained!=nil   -> must be kept (UID mismatch / foreign finalizer / changed object)
//	abandon!=nil    -> transient error; stop the whole pass without deleting
func (r *Reclaimer) confirmCandidate(ctx context.Context, direct client.Reader, candidate *ipamv1.IPAddress) (confirmed bool, retained *classification, abandon error) {
	claimKey := types.NamespacedName{Namespace: candidate.Namespace, Name: candidate.Spec.ClaimRef.Name}
	liveClaim := &ipamv1.IPAddressClaim{}
	err := direct.Get(ctx, claimKey, liveClaim)
	switch {
	case err == nil:
		// A same-name claim exists on the authoritative read.
		wantUID := candidate.Annotations[providerannotations.ClaimUIDAnnotation]
		if string(liveClaim.UID) == wantUID {
			// Cache lag hid a claim that genuinely owns this address.
			return false, nil, nil
		}
		if liveClaim.Status.AddressRef.Name == candidate.Name {
			return false, &classification{
				kind: classRetained, reason: ClaimUIDAnnotationStale,
				message: "direct read: live same-name claim references this address; annotation is stale",
			}, nil
		}
		return false, &classification{
			kind: classRetained, reason: ClaimUIDMismatch,
			message: "direct read: same-name claim with a different UID does not reference this address",
		}, nil
	case !apierrors.IsNotFound(err):
		return false, nil, err
	}

	// Claim confirmed absent. Re-read the address directly to detect concurrent change.
	directAddress := &ipamv1.IPAddress{}
	if err := direct.Get(ctx, client.ObjectKeyFromObject(candidate), directAddress); err != nil {
		if apierrors.IsNotFound(err) {
			// Already gone (e.g. a previous pass deleted it after a crash). Converged.
			return false, nil, nil
		}
		return false, nil, err
	}

	if !addressUnchanged(candidate, directAddress) {
		return false, &classification{
			kind: classRetained, reason: ConfirmationFailed,
			message: "address changed between the cached list and the direct read; retrying next pass",
		}, nil
	}

	// A terminating object may be blocked by a finalizer the provider does not own.
	if !directAddress.DeletionTimestamp.IsZero() {
		if hasForeignFinalizer(directAddress) {
			return false, &classification{
				kind: classRetained, reason: TerminatingForeignFinalizer,
				message: "address is terminating but held by a finalizer the provider does not own",
			}, nil
		}
	}

	return true, nil, nil
}

// deleteConfirmed removes the provider's protecting finalizer and deletes the address.
// Both operations tolerate NotFound so retries after a crash converge.
func (r *Reclaimer) deleteConfirmed(ctx context.Context, address *ipamv1.IPAddress) error {
	if controllerutil.ContainsFinalizer(address, protectingFinalizer) {
		patchBase := address.DeepCopy()
		controllerutil.RemoveFinalizer(address, protectingFinalizer)
		if err := r.Client.Patch(ctx, address, client.MergeFrom(patchBase)); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}

	if err := r.Client.Delete(ctx, address); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// addressUnchanged verifies the ownership-bearing fields did not change between the cached
// list and the direct read.
func addressUnchanged(cached, direct *ipamv1.IPAddress) bool {
	return cached.Spec.ClaimRef.Name == direct.Spec.ClaimRef.Name &&
		cached.Annotations[providerannotations.ClaimUIDAnnotation] ==
			direct.Annotations[providerannotations.ClaimUIDAnnotation] &&
		cached.UID == direct.UID
}

// hasForeignFinalizer reports whether any finalizer other than the provider's protecting
// finalizer is present.
func hasForeignFinalizer(address *ipamv1.IPAddress) bool {
	for _, f := range address.Finalizers {
		if f != protectingFinalizer {
			return true
		}
	}
	return false
}

// addRetained records a retained object. The bounded list is truncated later in finalize,
// but the total is always counted.
func (o *Outcome) addRetained(address ipamv1.IPAddress, reason OrphanReason, message string) {
	o.RetainedTotal++
	o.Retained = append(o.Retained, OrphanedAddress{
		Name:      address.Name,
		Namespace: address.Namespace,
		Address:   address.Spec.Address,
		Reason:    reason,
		Message:   message,
	})
}

// finalize sorts and truncates the retained list and sets the default requeue behaviour.
func (o *Outcome) finalize() {
	sort.SliceStable(o.Retained, func(i, j int) bool {
		if o.Retained[i].Namespace != o.Retained[j].Namespace {
			return o.Retained[i].Namespace < o.Retained[j].Namespace
		}
		return o.Retained[i].Name < o.Retained[j].Name
	})
	if len(o.Retained) > maxOrphanedStatusEntries {
		o.Retained = o.Retained[:maxOrphanedStatusEntries]
	}

	// An explicit action (deletes, batch, abandon) already set its own result.
	if o.Action != Noop {
		return
	}
	if o.RetainedTotal > 0 {
		o.Action = ResyncScheduled
		o.Result = ctrl.Result{RequeueAfter: resyncRequeue}
	}
}

// sortAddresses orders addresses deterministically by namespace then name so that batching
// and truncation are stable across passes.
func sortAddresses(addresses []ipamv1.IPAddress) {
	sort.Slice(addresses, func(i, j int) bool {
		if addresses[i].Namespace != addresses[j].Namespace {
			return addresses[i].Namespace < addresses[j].Namespace
		}
		return addresses[i].Name < addresses[j].Name
	})
}
