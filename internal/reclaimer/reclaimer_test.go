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
	"strconv"
	"testing"

	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ipamv1 "sigs.k8s.io/cluster-api/api/ipam/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	providerannotations "sigs.k8s.io/cluster-api-ipam-provider-in-cluster/pkg/annotations"
)

var ctx = context.Background()

// idx renders an index as a zero-padded two-character name suffix for deterministic order.
func idx(i int) string {
	return strconv.Itoa(i/10) + strconv.Itoa(i%10)
}

func keyOf(name string) client.ObjectKey {
	return types.NamespacedName{Namespace: testNamespace, Name: name}
}

func transientError() error {
	return apierrors.NewInternalError(errString("simulated transient API failure"))
}

type errString string

func (e errString) Error() string { return string(e) }

// 1. Claim deleted -> residual address confirmed via direct NotFound -> deleted once.
func TestReclaimDeletesResidualAfterDirectConfirmation(t *testing.T) {
	g := NewWithT(t)
	residual := address("residual", "10.0.0.10", withClaimUID("uid-1"),
		withFinalizers(protectingFinalizer))
	h := newHarness(t, residual)

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(outcome.Action).To(Equal(Deleted))
	g.Expect(outcome.Deleted).To(Equal([]client.ObjectKey{keyOf("residual")}))
	g.Expect(h.deletes()).To(HaveLen(1))

	surviving, err := h.survivingAddresses(ctx)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(surviving).To(BeEmpty())
}

// 2. Same-name claim recreated (new UID) but references the object: stale annotation,
// never deleted.
func TestReclaimRetainsStaleAnnotationAdoptedByLiveClaim(t *testing.T) {
	g := NewWithT(t)
	liveClaim := claim("new-uid", "same-name", "same-name")
	legacy := address("same-name", "10.0.0.11", withClaimUID("old-uid"),
		withFinalizers(protectingFinalizer))
	h := newHarness(t, liveClaim, legacy)

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(h.deletes()).To(BeEmpty())
	g.Expect(outcome.RetainedTotal).To(Equal(1))
	g.Expect(outcome.Retained[0].Reason).To(Equal(ClaimUIDAnnotationStale))

	surviving, err := h.survivingAddresses(ctx)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(surviving).To(HaveLen(1))
	g.Expect(surviving[0].Name).To(Equal("same-name"))
}

// 2b. Same-name claim exists with a different UID and does NOT reference the address:
// retained as a mismatch, never deleted.
func TestReclaimRetainsUIDMismatchWithoutAdoption(t *testing.T) {
	g := NewWithT(t)
	// Live claim references a different address name.
	liveClaim := claim("new-uid", "ambiguous", "other-address")
	ambiguous := address("ambiguous", "10.0.0.12", withClaimUID("old-uid"),
		withFinalizers(protectingFinalizer))
	h := newHarness(t, liveClaim, ambiguous)

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(h.deletes()).To(BeEmpty())
	g.Expect(outcome.Retained[0].Reason).To(Equal(ClaimUIDMismatch))
}

// 3. Cache lag hides a just-created claim; the direct read sees it -> no delete.
func TestReclaimCacheLagDoesNotDeleteLiveClaimAddress(t *testing.T) {
	g := NewWithT(t)
	liveClaim := claim("uid-9", "fresh", "fresh")
	fresh := address("fresh", "10.0.0.13", withClaimUID("uid-9"),
		withFinalizers(protectingFinalizer))
	h := newHarness(t, liveClaim, fresh)
	h.hideCachedGet(keyOf("fresh"))

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(h.deletes()).To(BeEmpty())
	g.Expect(outcome.Action).To(Equal(Noop))
	g.Expect(outcome.RetainedTotal).To(Equal(0))

	surviving, err := h.survivingAddresses(ctx)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(surviving).To(HaveLen(1))
}

// 4a. Finalizer already removed (crash between patch and delete): delete retried.
func TestReclaimRetriesDeleteAfterFinalizerAlreadyRemoved(t *testing.T) {
	g := NewWithT(t)
	noFinalizer := address("crash-1", "10.0.0.14", withClaimUID("uid-c1"))
	h := newHarness(t, noFinalizer)

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(outcome.Action).To(Equal(Deleted))
	g.Expect(h.deletes()).To(Equal([]client.ObjectKey{keyOf("crash-1")}))
}

// 4b. The object disappears between the cached list and the direct re-read (another actor
// deleted it): confirmation sees NotFound, so the reclaimer does not issue its own Delete
// and reports no error. The pass is a safe no-op for this object.
func TestReclaimTreatsObjectVanishingDuringPassAsConverged(t *testing.T) {
	g := NewWithT(t)
	residual := address("gone", "10.0.0.15", withClaimUID("uid-gone"),
		withFinalizers(protectingFinalizer))
	h := newHarness(t, residual)

	// The list (cached) still returns the object, but the authoritative direct read of the
	// address returns NotFound, emulating a delete that landed between list and confirm.
	h.direct = &vanishingAddressReader{Reader: h.backing, vanish: keyOf("gone")}

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	// Nothing was deleted by the reclaimer: the object had already been removed.
	g.Expect(h.deletes()).To(BeEmpty())
	g.Expect(outcome.Deleted).To(BeEmpty())
	g.Expect(outcome.RetainedTotal).To(Equal(0))
}

// 4c. Terminating object held by a foreign finalizer: retained, not force-deleted.
func TestReclaimRetainsTerminatingObjectWithForeignFinalizer(t *testing.T) {
	g := NewWithT(t)
	foreign := address("foreign", "10.0.0.16", withClaimUID("uid-f"),
		withFinalizers(protectingFinalizer, "someone.else/finalizer"),
		withDeletionTimestamp())
	h := newHarness(t, foreign)

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(h.deletes()).To(BeEmpty())
	g.Expect(outcome.Retained[0].Reason).To(Equal(TerminatingForeignFinalizer))
}

// 5a. Missing claimRef: retained, never a candidate.
func TestReclaimRetainsAddressWithMissingClaimRef(t *testing.T) {
	g := NewWithT(t)
	corrupt := address("corrupt", "10.0.0.17", withClaimUID("uid-x"),
		withFinalizers(protectingFinalizer))
	corrupt.Spec.ClaimRef.Name = ""
	h := newHarness(t, corrupt)

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(h.deletes()).To(BeEmpty())
	g.Expect(outcome.Retained[0].Reason).To(Equal(ClaimRefMissing))
}

// 5b. Legacy object with matching controller ownerRef UID: backfilled, not deleted.
func TestReclaimBackfillsLegacyWithMatchingOwnerRefUID(t *testing.T) {
	g := NewWithT(t)
	liveClaim := claim("uid-legacy", "legacy-a", "")
	legacy := address("legacy-a", "10.0.0.18",
		withFinalizers(protectingFinalizer),
		withControllerOwner("uid-legacy", "legacy-a"))
	h := newHarness(t, liveClaim, legacy)

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(h.deletes()).To(BeEmpty())
	g.Expect(outcome.Action).To(Equal(Noop))

	updated := &ipamv1.IPAddress{}
	g.Expect(h.backing.Get(ctx, keyOf("legacy-a"), updated)).To(Succeed())
	g.Expect(updated.Annotations[providerannotations.ClaimUIDAnnotation]).To(Equal("uid-legacy"))
}

// 5c. Legacy object with addressRef back-pointer and no conflicting ownerRef: backfilled.
func TestReclaimBackfillsLegacyWithAddressRefAndNoConflictingOwner(t *testing.T) {
	g := NewWithT(t)
	liveClaim := claim("uid-b", "legacy-b", "legacy-b")
	legacy := address("legacy-b", "10.0.0.19", withFinalizers(protectingFinalizer))
	h := newHarness(t, liveClaim, legacy)

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(h.deletes()).To(BeEmpty())
	g.Expect(outcome.Action).To(Equal(Noop))

	updated := &ipamv1.IPAddress{}
	g.Expect(h.backing.Get(ctx, keyOf("legacy-b"), updated)).To(Succeed())
	g.Expect(updated.Annotations[providerannotations.ClaimUIDAnnotation]).To(Equal("uid-b"))
}

// 5d. Legacy object with a conflicting ownerRef and no usable evidence: retained.
func TestReclaimRetainsLegacyWithConflictingOwnerRef(t *testing.T) {
	g := NewWithT(t)
	// Live claim has a different UID than the stale controller owner reference, and does
	// not point back at this address.
	liveClaim := claim("new-uid", "legacy-c", "elsewhere")
	legacy := address("legacy-c", "10.0.0.20", withFinalizers(protectingFinalizer),
		withControllerOwner("stale-uid", "legacy-c"))
	h := newHarness(t, liveClaim, legacy)

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(h.deletes()).To(BeEmpty())
	g.Expect(outcome.Retained[0].Reason).To(Equal(ClaimUIDAnnotationMissing))
}

// 5e. Legacy object and no same-name claim at all: retained.
func TestReclaimRetainsLegacyWithNoLiveClaim(t *testing.T) {
	g := NewWithT(t)
	legacy := address("legacy-d", "10.0.0.21", withFinalizers(protectingFinalizer))
	h := newHarness(t, legacy)

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(h.deletes()).To(BeEmpty())
	g.Expect(outcome.Retained[0].Reason).To(Equal(ClaimUIDAnnotationMissing))
}

// 6. Bulk: more residuals than the per-pass cap -> bounded first pass, then drain.
func TestReclaimBatchesBulkResiduals(t *testing.T) {
	g := NewWithT(t)
	objs := make([]client.Object, 0, maxCandidatesPerPass+10)
	for i := range maxCandidatesPerPass + 10 {
		objs = append(objs, address(
			"bulk-"+idx(i), "10.1.0."+strconv.Itoa(i), withClaimUID(types.UID("u"+strconv.Itoa(i))),
			withFinalizers(protectingFinalizer)))
	}
	h := newHarness(t, objs...)

	first, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(first.Action).To(Equal(BatchDeferred))
	g.Expect(first.Deleted).To(HaveLen(maxCandidatesPerPass))
	g.Expect(first.Result.RequeueAfter).To(Equal(batchRequeue))
	g.Expect(h.deletes()).To(HaveLen(maxCandidatesPerPass))

	second, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(second.Deleted).To(HaveLen(10))
	g.Expect(h.deletes()).To(HaveLen(maxCandidatesPerPass + 10))

	surviving, err := h.survivingAddresses(ctx)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(surviving).To(BeEmpty())
}

// 6b. Retained objects are bounded in the status list but fully counted.
func TestReclaimBoundsRetainedListButCountsAll(t *testing.T) {
	g := NewWithT(t)
	objs := make([]client.Object, 0, maxOrphanedStatusEntries+5)
	for i := range maxOrphanedStatusEntries + 5 {
		a := address("corrupt-"+idx(i), "10.2.0."+strconv.Itoa(i), withClaimUID("u"),
			withFinalizers(protectingFinalizer))
		a.Spec.ClaimRef.Name = ""
		objs = append(objs, a)
	}
	h := newHarness(t, objs...)

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(h.deletes()).To(BeEmpty())
	g.Expect(outcome.RetainedTotal).To(Equal(maxOrphanedStatusEntries + 5))
	g.Expect(outcome.Retained).To(HaveLen(maxOrphanedStatusEntries))
}

// 7. Transient direct-reader error: abandon with requeue and nil error, zero deletes.
func TestReclaimAbandonsOnTransientConfirmationError(t *testing.T) {
	g := NewWithT(t)
	residual := address("flaky", "10.0.0.22", withClaimUID("uid-flaky"),
		withFinalizers(protectingFinalizer))
	h := newHarness(t, residual)
	h.setDirectGetErr(transientError())

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(outcome.Action).To(Equal(Abandon))
	g.Expect(outcome.Result.RequeueAfter).To(Equal(abandonRequeue))
	g.Expect(h.deletes()).To(BeEmpty())
	g.Expect(outcome.Retained[0].Reason).To(Equal(ConfirmationFailed))

	// Once the transient error clears, the next pass deletes.
	h.setDirectGetErr(nil)
	outcome2, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(outcome2.Action).To(Equal(Deleted))
	g.Expect(h.deletes()).To(HaveLen(1))
}

// 7b. A transient cached claim Get error retains (never makes a candidate), nil error.
func TestReclaimRetainsOnTransientCachedGetError(t *testing.T) {
	g := NewWithT(t)
	annotated := address("cache-err", "10.0.0.23", withClaimUID("uid-ce"),
		withFinalizers(protectingFinalizer))
	h := newHarness(t, annotated)
	h.setCachedGetErr(transientError())

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(h.deletes()).To(BeEmpty())
	g.Expect(outcome.Retained[0].Reason).To(Equal(ConfirmationFailed))
}

// 8. Global (cluster-scoped) pool: empty namespace lists across namespaces; the claim is
// looked up in the address's own namespace.
func TestReclaimGlobalPoolUsesAddressNamespaceForClaim(t *testing.T) {
	g := NewWithT(t)
	residual := address("global-1", "10.0.0.24", withClaimUID("uid-g1"),
		withFinalizers(protectingFinalizer))
	h := newHarness(t, residual)

	// Empty pool namespace -> cluster-wide scope; the address still carries its namespace
	// and the direct claim lookup uses it.
	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, "", h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(outcome.Action).To(Equal(Deleted))
	g.Expect(h.deletes()).To(Equal([]client.ObjectKey{keyOf("global-1")}))
}

// 9. Healthy address whose claim UID matches is left untouched and schedules nothing.
func TestReclaimLeavesHealthyAddressAlone(t *testing.T) {
	g := NewWithT(t)
	liveClaim := claim("uid-ok", "healthy", "healthy")
	healthy := address("healthy", "10.0.0.25", withClaimUID("uid-ok"),
		withFinalizers(protectingFinalizer))
	h := newHarness(t, liveClaim, healthy)

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(outcome.Action).To(Equal(Noop))
	g.Expect(outcome.Result).To(BeZero())
	g.Expect(h.deletes()).To(BeEmpty())
}

// 10. Resync is scheduled when only retained objects exist (no deletions).
func TestReclaimSchedulesResyncForRetainedOnly(t *testing.T) {
	g := NewWithT(t)
	corrupt := address("retained-only", "10.0.0.26", withClaimUID("u"),
		withFinalizers(protectingFinalizer))
	corrupt.Spec.ClaimRef.Name = ""
	h := newHarness(t, corrupt)

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, false)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(outcome.Action).To(Equal(ResyncScheduled))
	g.Expect(outcome.Result.RequeueAfter).To(Equal(resyncRequeue))
}

// 11. Deleting pool: legacy backfill is skipped but confirmed residuals are deleted.
func TestReclaimOnDeletingPoolSkipsBackfillButDeletesResiduals(t *testing.T) {
	g := NewWithT(t)
	liveClaim := claim("uid-leg", "legacy-del", "")
	legacy := address("legacy-del", "10.0.0.27", withFinalizers(protectingFinalizer),
		withControllerOwner("uid-leg", "legacy-del"))
	residual := address("residual-del", "10.0.0.28", withClaimUID("uid-rd"),
		withFinalizers(protectingFinalizer))
	h := newHarness(t, liveClaim, legacy, residual)

	outcome, err := h.reclaimer().ReclaimOrphanedAddresses(ctx, testNamespace, h.poolRef, true)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(h.deletes()).To(Equal([]client.ObjectKey{keyOf("residual-del")}))
	g.Expect(outcome.RetainedTotal).To(Equal(1))
	g.Expect(outcome.Retained[0].Reason).To(Equal(ClaimUIDAnnotationMissing))

	updated := &ipamv1.IPAddress{}
	g.Expect(h.backing.Get(ctx, keyOf("legacy-del"), updated)).To(Succeed())
	_, backfilled := updated.Annotations[providerannotations.ClaimUIDAnnotation]
	g.Expect(backfilled).To(BeFalse())
}
