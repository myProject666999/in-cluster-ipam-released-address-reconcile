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
	"time"

	ipamv1 "sigs.k8s.io/cluster-api/api/ipam/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/pkg/ipamutil"
)

const (
	// maxCandidatesPerPass bounds both the number of second confirmations and the number
	// of deletions a single reconcile pass performs. Work left over is requeued.
	maxCandidatesPerPass = 50

	// maxOrphanedStatusEntries bounds the number of retained objects embedded in pool
	// status; OrphanedAddressCount always reports the unbounded total.
	maxOrphanedStatusEntries = 100

	// batchRequeue is the delay before resuming when the per-pass cap was reached.
	batchRequeue = 1 * time.Second

	// abandonRequeue is the delay after a transient confirmation failure.
	abandonRequeue = 5 * time.Second

	// resyncRequeue provides a safety net for residue that no watch event surfaced.
	resyncRequeue = 10 * time.Minute
)

// Action describes what a reclaim pass did, so the calling pool reconciler can choose the
// right requeue behaviour.
type Action string

const (
	// Noop means nothing had to be changed.
	Noop Action = "Noop"
	// Deleted means one or more residual addresses were confirmed and deleted.
	Deleted Action = "Deleted"
	// Abandon means a transient confirmation error stopped the pass early. Nothing was
	// deleted and the error is swallowed: the pass is requeued instead of failing.
	Abandon Action = "Abandon"
	// BatchDeferred means the per-pass cap was reached while candidates remain.
	BatchDeferred Action = "BatchDeferred"
	// ResyncScheduled means no deletion happened but retained/unconfirmed objects warrant
	// a periodic revisit.
	ResyncScheduled Action = "ResyncScheduled"
)

// OrphanReason is the machine-readable classification of a retained address.
type OrphanReason string

const (
	// ClaimRefMissing means spec.claimRef.name is empty or otherwise unusable.
	ClaimRefMissing OrphanReason = "ClaimRefMissing"
	// ClaimUIDAnnotationMissing means a legacy address has no claim-UID annotation and its
	// ownership could not be established with trusted evidence.
	ClaimUIDAnnotationMissing OrphanReason = "ClaimUIDAnnotationMissing"
	// ClaimUIDAnnotationStale means a live same-name claim has adopted the address but the
	// annotation still carries the previous claim's UID. The address is healthy; the claim
	// reconciler rewrites the annotation. It is never a deletion candidate.
	ClaimUIDAnnotationStale OrphanReason = "ClaimUIDAnnotationStale"
	// ClaimUIDMismatch means a same-name claim with a different UID exists but does not
	// reference this address. The object is retained rather than guessed at.
	ClaimUIDMismatch OrphanReason = "ClaimUIDMismatch"
	// ConfirmationFailed means the non-cached confirmation could not be completed.
	ConfirmationFailed OrphanReason = "ConfirmationFailed"
	// TerminatingForeignFinalizer means the address is deleting but blocked by a finalizer
	// the provider does not own, so the provider must not force it.
	TerminatingForeignFinalizer OrphanReason = "TerminatingForeignFinalizer"
)

// OrphanedAddress is the internal representation of a retained object, mapped onto the
// v1alpha2.OrphanedIPAddress API type by the calling reconciler.
type OrphanedAddress struct {
	Name      string
	Namespace string
	Address   string
	Reason    OrphanReason
	Message   string
}

// Outcome is the result of a reclaim pass. The reclaimer never mutates the pool object;
// the caller owns the single pool-status write.
type Outcome struct {
	// Action classifies the pass.
	Action Action
	// Result carries the requeue the caller should return. It is the zero result when no
	// requeue is needed.
	Result ctrl.Result
	// Deleted lists the object keys that were actually deleted, in order.
	Deleted []client.ObjectKey
	// Retained is the bounded (maxOrphanedStatusEntries) list of retained objects for pool
	// status.
	Retained []OrphanedAddress
	// RetainedTotal is the unbounded number of retained objects.
	RetainedTotal int
}

// Reclaimer reclaims residual IPAddress objects for in-cluster pools.
type Reclaimer struct {
	// Client is the (cached) controller-runtime client used for the initial list, cached
	// claim lookups, backfill writes and deletions.
	Client client.Client
	// DirectReader is a non-cached reader used for the second confirmation. If nil, Client
	// is used; production wiring passes mgr.GetAPIReader().
	DirectReader client.Reader
}

// ReclaimOrphanedAddresses lists the addresses of a pool, classifies them against their
// claims, confirms and deletes proven residual objects (bounded per pass), and reports the
// objects it deliberately retained together with a requeue result.
//
// poolNamespace is empty for cluster-scoped pools. poolDeleting disables migration
// backfill writes while the pool itself is being torn down. This method never returns an
// error for a failed confirmation (that is an Abandon outcome); only hard list/type errors
// are returned.
func (r *Reclaimer) ReclaimOrphanedAddresses(ctx context.Context, poolNamespace string, poolRef ipamv1.IPPoolReference, poolDeleting bool) (Outcome, error) {
	log := ctrl.LoggerFrom(ctx)

	outcome := Outcome{Action: Noop}

	addresses, err := listPoolAddresses(ctx, r.Client, poolNamespace, poolRef)
	if err != nil {
		return outcome, err
	}

	candidates := make([]ipamv1.IPAddress, 0)
	for i := range addresses {
		address := addresses[i]
		switch classification := r.classify(ctx, &address, poolDeleting); classification.kind {
		case classHealthy, classBackfilled:
			// Owned by a live claim; nothing to do.
			continue
		case classCandidate:
			candidates = append(candidates, address)
		case classRetained:
			outcome.addRetained(address, classification.reason, classification.message)
			log.Info("Retaining IPAddress with unproven ownership",
				"ipAddress", client.ObjectKeyFromObject(&address).String(),
				"address", address.Spec.Address,
				"reason", classification.reason,
				"message", classification.message)
		}
	}

	r.confirmAndDelete(ctx, &outcome, candidates)
	outcome.finalize()
	return outcome, nil
}

// direct returns the non-cached reader, falling back to the cached client when no direct
// reader was configured.
func (r *Reclaimer) direct() client.Reader {
	if r.DirectReader != nil {
		return r.DirectReader
	}
	return r.Client
}

// protectingFinalizer aliases the finalizer the claim reconciler adds to addresses, so the
// reclaimer can remove it before deleting a confirmed residual.
const protectingFinalizer = ipamutil.ProtectAddressFinalizer
