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

// Package annotations holds annotation keys shared between the claim reconciler
// (pkg/ipamutil) and the released-address reclaimer (internal/reclaimer).
//
// It intentionally lives under pkg/ rather than internal/ so that both packages can
// import it: Go forbids a package rooted at pkg/ from importing a package under internal/.
package annotations

// ClaimUIDAnnotation records the UID of the IPAddressClaim an IPAddress was created for.
//
// The CAPI IPAddress.spec.claimRef only carries the claim's name
// (sigs.k8s.io/cluster-api/api/ipam/v1beta2.IPAddressClaimReference), and a claim can be
// deleted and recreated under the same name with a different UID. The UID recorded here is
// therefore the authoritative ownership evidence used to decide whether an address whose
// claim has disappeared is a residual object that is safe to delete.
const ClaimUIDAnnotation = "ipam.cluster.x-k8s.io/claim-uid"
