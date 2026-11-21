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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ipamv1 "sigs.k8s.io/cluster-api/api/ipam/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/internal/poolutil"
	providerannotations "sigs.k8s.io/cluster-api-ipam-provider-in-cluster/pkg/annotations"
)

type classificationKind string

const (
	classHealthy    classificationKind = "healthy"
	classBackfilled classificationKind = "backfilled"
	classCandidate  classificationKind = "candidate"
	classRetained   classificationKind = "retained"
)

type classification struct {
	kind    classificationKind
	reason  OrphanReason
	message string
}

// classify decides what a single address is, based on the cached view. It may perform a
// single migration backfill write (the only write it does). It never deletes.
func (r *Reclaimer) classify(ctx context.Context, address *ipamv1.IPAddress, poolDeleting bool) classification {
	claimName := address.Spec.ClaimRef.Name
	if claimName == "" {
		return classification{
			kind:    classRetained,
			reason:  ClaimRefMissing,
			message: "IPAddress has no spec.claimRef.name; ownership cannot be determined",
		}
	}

	claimKey := types.NamespacedName{Namespace: address.Namespace, Name: claimName}
	claim := &ipamv1.IPAddressClaim{}
	claimErr := r.Client.Get(ctx, claimKey, claim)
	if claimErr != nil && !apierrors.IsNotFound(claimErr) {
		// A transient cached-read error: do not treat the address as a candidate. Retain it
		// this pass; the pool is re-reconciled on the next watch/resync.
		return classification{
			kind:    classRetained,
			reason:  ConfirmationFailed,
			message: "failed to read referenced claim from cache: " + claimErr.Error(),
		}
	}
	claimMissing := apierrors.IsNotFound(claimErr)

	wantUID, hasUID := address.Annotations[providerannotations.ClaimUIDAnnotation]
	if !hasUID || wantUID == "" {
		// Legacy object: no UID evidence. It is never a deletion candidate.
		return r.classifyLegacy(ctx, address, claim, claimMissing, poolDeleting)
	}

	if claimMissing {
		// The cached view has no claim. Cache lag could hide a just-created claim, so this
		// is only a candidate; the direct read makes the final decision.
		return classification{kind: classCandidate}
	}

	if string(claim.UID) == wantUID {
		return classification{kind: classHealthy}
	}

	// A same-name claim exists but with a different UID. Never delete on this evidence.
	// If the live claim has adopted this exact address object, the annotation is merely
	// stale and the claim reconciler rewrites it; otherwise the object is ambiguous.
	if claim.Status.AddressRef.Name == address.Name {
		return classification{
			kind: classRetained, reason: ClaimUIDAnnotationStale,
			message: "live same-name claim references this address but its UID differs from the claim-uid annotation",
		}
	}
	return classification{
		kind: classRetained, reason: ClaimUIDMismatch,
		message: "same-name claim exists with a different UID and does not reference this address",
	}
}

// classifyLegacy handles addresses created before the UID annotation existed. They are
// never deleted; the annotation is backfilled only when trusted evidence exists.
func (r *Reclaimer) classifyLegacy(ctx context.Context, address *ipamv1.IPAddress, claim *ipamv1.IPAddressClaim, claimMissing, poolDeleting bool) classification {
	log := ctrl.LoggerFrom(ctx)

	if claimMissing {
		return classification{
			kind: classRetained, reason: ClaimUIDAnnotationMissing,
			message: "legacy IPAddress has no claim-uid annotation and no same-name claim exists",
		}
	}

	// Trusted evidence, OR-rule:
	//  (a) the controller owner reference carries the live claim's UID, or
	//  (b) the live claim points back at this address and there is no conflicting
	//      controller owner reference.
	ownerRef, hasControllerRef := controllerOwnerRef(address)
	evidenceA := hasControllerRef && ownerRef.UID == claim.UID
	evidenceB := claim.Status.AddressRef.Name == address.Name &&
		(!hasControllerRef || ownerRef.UID == claim.UID)

	if !evidenceA && !evidenceB {
		return classification{
			kind: classRetained, reason: ClaimUIDAnnotationMissing,
			message: "legacy IPAddress has no claim-uid annotation and no trusted bidirectional ownership evidence",
		}
	}

	if poolDeleting {
		// Avoid migration churn while tearing the pool down; confirmation/deletion of
		// genuine leftovers still proceeds, but this legacy object is retained.
		return classification{
			kind: classRetained, reason: ClaimUIDAnnotationMissing,
			message: "legacy IPAddress ownership is provable but backfill is skipped while the pool is deleting",
		}
	}

	if err := r.backfillClaimUID(ctx, address, claim.UID); err != nil {
		log.Error(err, "Failed to backfill claim-uid annotation on legacy IPAddress",
			"ipAddress", client.ObjectKeyFromObject(address).String())
		return classification{
			kind: classRetained, reason: ClaimUIDAnnotationMissing,
			message: "failed to backfill claim-uid annotation: " + err.Error(),
		}
	}

	log.Info("Backfilled claim-uid annotation on legacy IPAddress from trusted ownership evidence",
		"ipAddress", client.ObjectKeyFromObject(address).String(), "claimUID", claim.UID)
	return classification{kind: classBackfilled}
}

// backfillClaimUID writes the claim UID annotation without changing anything else.
func (r *Reclaimer) backfillClaimUID(ctx context.Context, address *ipamv1.IPAddress, claimUID types.UID) error {
	base := address.DeepCopy()
	if address.Annotations == nil {
		address.Annotations = map[string]string{}
	}
	address.Annotations[providerannotations.ClaimUIDAnnotation] = string(claimUID)
	return r.Client.Patch(ctx, address, client.MergeFrom(base))
}

// controllerOwnerRef returns the controller owner reference, if any.
func controllerOwnerRef(address *ipamv1.IPAddress) (metav1.OwnerReference, bool) {
	for _, ref := range address.OwnerReferences {
		if ref.Controller != nil && *ref.Controller {
			return ref, true
		}
	}
	return metav1.OwnerReference{}, false
}

// listPoolAddresses fetches the addresses of a pool through the cached client and returns
// them in a deterministic namespace/name order.
func listPoolAddresses(ctx context.Context, c client.Client, poolNamespace string, poolRef ipamv1.IPPoolReference) ([]ipamv1.IPAddress, error) {
	addresses, err := poolutil.ListAddressesInUse(ctx, c, poolNamespace, poolRef)
	if err != nil {
		return nil, err
	}
	sortAddresses(addresses)
	return addresses, nil
}
