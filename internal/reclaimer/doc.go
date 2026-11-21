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

// Package reclaimer finds and removes IPAddress objects whose owning IPAddressClaim no
// longer exists, without ever deleting an address whose ownership cannot be proven.
//
// # Why this package exists
//
// An IPAddress.spec.claimRef carries only the claim's name; names can be reused by a
// recreated claim that has a different UID. Deleting an address on name-only evidence can
// therefore free an address that a new, same-named claim is already using, eventually
// causing two machines to share one IP. The authoritative identity is the claim UID,
// recorded by the claim reconciler in the
// "ipam.cluster.x-k8s.io/claim-uid" annotation.
//
// # Safety model
//
//   - The cached list is used only to classify addresses. Cache lag may hide a claim that
//     was just created, so a cached "claim not found" never causes a deletion by itself.
//   - Every deletion candidate is confirmed with a direct (non-cached) read of both the
//     claim and the address. Only a direct NotFound of the claim, combined with an
//     unchanged address, reaches the delete path.
//   - A failed confirmation abandons the whole pass (requeue, no error). Abandonment is a
//     normal outcome, not an exception.
//   - Legacy objects without a UID annotation, and objects with missing or corrupt
//     references, are retained and reported instead of being guessed at.
//   - Deletions are bounded per pass so that bulk cleanup of hundreds or thousands of
//     residual objects does not burst the API server.
//
// See docs/released-address-reconcile.md for the full state machine and crash-recovery
// guarantees.
package reclaimer
