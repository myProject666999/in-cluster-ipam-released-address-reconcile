# Released-Address Reconciliation

This document describes how the in-cluster IPAM provider reclaims `IPAddress` objects
whose owning `IPAddressClaim` has disappeared, without ever deleting an address whose
ownership cannot be proven. The implementation lives in
[`internal/reclaimer`](../internal/reclaimer) and is driven by the pool reconcilers.

## Why reclaim is needed

An `IPAddress` is normally deleted through the claim lifecycle: the claim's
`ReleaseAddress` finalizer removes the address's `ProtectAddress` finalizer and deletes
the object. If that path does not complete — a controller crash, a removed finalizer, or a
manual object edit — the `IPAddress` can remain after its claim is gone. It is still
listed by `poolutil.ListAddressesInUse`, so it permanently consumes pool capacity and its
address can never be reallocated.

A naive sweep that matches addresses to claims **by name** is unsafe. The
`IPAddress.spec.claimRef` carries only the claim's name, and a claim can be deleted and
recreated under the same name with a different UID. Deleting on name-only evidence could
free an address that the new claim is already using, eventually placing the same IP on two
machines — an intermittent, hard-to-trace network fault.

## Ownership model

Identity is the claim **UID**, not its name. When the claim reconciler creates or patches
an address (`ClaimReconciler.Reconcile`), it writes the annotation

```
ipam.cluster.x-k8s.io/claim-uid: <claim UID>
```

(defined as `annotations.ClaimUIDAnnotation`). The claim's controller `OwnerReference` on
the address also carries a UID and is validated by Kubernetes garbage collection; it is
used as independent evidence during migration.

## Reconciliation placement

The reclaimer runs from the **pool** reconcilers
(`genericReconcile` / `genericPrefixReconcile`), after the address list is loaded but
**before** the pool's deletion-timestamp short-circuit. This ordering matters: a pool
being deleted waits for its address count to reach zero, so reclamation must still run
while the pool is terminating or such a pool could never drain. The reclaimer never
mutates the pool object; it returns an `Outcome` and the pool reconciler performs the
single status write.

## Classification and confirmation state machine

The cached list is used only to *classify* addresses. A cached read can lag behind the API
server, so a cached "claim not found" never causes a deletion by itself.

### Cached classification

1. `spec.claimRef.name` is empty → **retain**, reason `ClaimRefMissing`. Never a candidate.
2. The claim-UID annotation is missing (a pre-upgrade object) → never a candidate, never
   deleted. See [migration](#migration-of-legacy-objects).
3. The annotation is present. Read the same-name claim from the cache:
   - same UID → healthy, skip;
   - not found → **candidate** (`ClaimNotFound`);
   - different UID → never delete. If the live claim's `status.addressRef` points at this
     address, it has adopted the object: **retain** as `ClaimUIDAnnotationStale` (the claim
     reconciler rewrites the annotation). Otherwise **retain** as `ClaimUIDMismatch`.

### Second confirmation (direct, non-cached reads)

Each candidate is confirmed with the API reader (`mgr.GetAPIReader()`), bounded to
`maxCandidatesPerPass = 50` per pass.

1. Direct GET of the same-name claim:
   - not found → the claim genuinely does not exist; continue;
   - found with the annotation's UID → cache lag; drop the candidate, delete nothing;
   - found with a different UID → apply the stale/mismatch rule above, delete nothing;
   - any other error → **abandon** the pass: requeue after `abandonRequeue = 5s`, return no
     error, and perform no further deletions. Abandonment is a normal path, not an error.
2. Direct re-read of the address and re-check that `claimRef`, the UID annotation and the
   object UID are unchanged. A change means a concurrent actor modified it → skip it this
   pass. A not-found result means it was already removed → treat as converged, no delete.
3. If the address is already terminating:
   - blocked only by the provider's `ProtectAddress` finalizer (or no foreign finalizer) →
     remove that finalizer and re-issue the delete;
   - blocked by a finalizer the provider does not own → **retain** as
     `TerminatingForeignFinalizer`; the provider must not force another component's cleanup.
4. Only now remove `ProtectAddress` (merge patch, not-found tolerated) and call `Delete`
   (not-found tolerated). Every deletion emits a structured audit log containing the
   address name/namespace/IP, the pool reference, the claim name, the expected claim UID
   and the reason — a durable explanation of why the object was judged residual.

## Migration of legacy objects

Objects created before the UID annotation existed carry only a name. They are **never**
bound to a same-name claim by name alone, because the original claim may already have been
replaced by a same-named object.

The annotation is backfilled only when trusted evidence exists (an OR rule):

- **(a)** the address's *controller* owner reference carries the live claim's UID — strong,
  GC-validated proof; or
- **(b)** the live claim's `status.addressRef` points back at this address **and** there is
  no conflicting controller owner reference.

A conflicting owner reference, a missing/corrupt claim reference, a malformed annotation,
or the absence of any same-name claim means the object is **retained with a warning**
(`ClaimUIDAnnotationMissing`) for an explicit, operator-driven migration flow. It is never
auto-deleted. Backfill writes are also skipped while the pool itself is being deleted, to
avoid migration churn during teardown.

## Batching, rate limiting and cost

- At most `maxCandidatesPerPass = 50` candidates are confirmed (and therefore at most 50
  objects deleted) per reconcile. Remaining candidates return `BatchDeferred` with a
  `batchRequeue = 1s` delay, so a residue of hundreds or thousands drains in bounded waves
  rather than bursting the API.
- A pass that only retained objects returns `ResyncScheduled`
  (`resyncRequeue = 10m`) as a safety net for residue no watch event surfaced. The existing
  IPAddress→pool watch already enqueues the pool on ordinary changes.
- Cost per confirmed candidate: up to two direct GETs (claim + address), one finalizer
  patch and one DELETE. Classification costs one cached GET per listed address. The
  effective ceiling is roughly 50 objects/second/pool.

### Capacity converges per batch

The pool reconciler re-lists addresses after reclamation and recomputes
`total/used/free` in the same reconcile, so the reported capacity improves after every
batch instead of only when the whole sweep finishes.

## Visibility of retained objects

Retained objects keep counting against `used` capacity and are also reported on the pool
status:

- `status.orphanedAddressCount` — the unbounded true count;
- `status.orphanedAddresses` — up to 100 entries (`maxOrphanedStatusEntries`), each with
  name, namespace, address, a machine-readable reason and a message. The list is
  `x-kubernetes-list-type: atomic` and deterministically ordered.

This prevents retained objects from silently holding capacity. Each deletion additionally
produces an audit log line, and each retained classification is logged with its reason.

## Crash recovery and ordering guarantees

Every operation is idempotent, so re-processing after any interruption converges:

| Interruption point | On restart |
| --- | --- |
| Before the delete call | The object is still present and reclassified next pass. |
| After finalizer removal, before `Delete` | Confirmation repeats; the absent finalizer is tolerated and `Delete` is retried (not-found is OK). |
| After `Delete`, before the pool status patch | The next list no longer contains the address and status converges. |
| Claim already gone | The pool-driven reclaimer still finds and cleans the address; the claim object itself is never deleted here. |
| Address already cleaned | The status can never permanently claim it is still held, because status is recomputed from the fresh list each pass. |

The existing claim-finalizer → address-deletion order in `reconcileDelete` is unchanged;
the reclaimer only handles objects the claim lifecycle can no longer reach.

### Interaction with the address-reuse grace period

`IPAddressClaimHandler.ReleaseAddress` delays deletion while a reuse grace period is
configured, but only **while the claim still exists**. The reclaimer never deletes an
address whose claim exists, so it cannot bypass the grace window. Once the claim is gone,
the grace period has already elapsed by definition.

## Testing

`internal/reclaimer/reclaimer_test.go` uses a fake client with deliberately divergent
cached and direct views (controller-runtime `interceptor.Funcs` over one indexed fake
tracker). It covers residual deletion, same-name recreation, cache lag, crash recovery,
missing references and migration evidence, terminating objects with foreign finalizers,
bulk batching and transient-reader abandonment — asserting both the surviving object set
and the exact sequence of delete calls. These tests require no envtest (API server)
assets. The end-to-end controller behavior is covered by the envtest Ginkgo suite under
`internal/controllers`.
