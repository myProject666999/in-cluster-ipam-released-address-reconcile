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
	"sync"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ipamv1 "sigs.k8s.io/cluster-api/api/ipam/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/api/v1alpha2"
	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/internal/index"
)

const (
	testNamespace = "test-ns"
	testPoolName  = "test-pool"
)

// harness bundles the three views exercised by the reclaimer:
//   - backing/direct: the authoritative full view (stands in for the API server)
//   - cached: a view whose Get can lag behind the authoritative view (cache staleness)
//   - recorder: captures every Delete call against the backing tracker
type harness struct {
	poolRef ipamv1.IPPoolReference

	backing client.WithWatch
	direct  client.Reader

	mu           sync.Mutex
	hiddenGets   map[types.NamespacedName]bool // keys the cached Get must report as NotFound
	getErr       error                         // forced error for cached claim Gets
	directGetErr error                         // forced error for direct claim Gets
	deleteCalls  []client.ObjectKey
}

func newHarness(t *testing.T, objs ...client.Object) *harness {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := ipamv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add ipam scheme: %v", err)
	}
	if err := v1alpha2.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1alpha2 scheme: %v", err)
	}

	poolRef := ipamv1.IPPoolReference{
		APIGroup: v1alpha2.GroupVersion.Group,
		Kind:     "InClusterIPPool",
		Name:     testPoolName,
	}

	recorder := &harness{
		poolRef:    poolRef,
		hiddenGets: map[types.NamespacedName]bool{},
	}

	backing := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithIndex(&ipamv1.IPAddress{}, index.IPAddressPoolRefCombinedField, index.IPAddressByCombinedPoolRef).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				recorder.recordDelete(client.ObjectKeyFromObject(obj))
				return c.Delete(ctx, obj, opts...)
			},
		}).
		Build()

	recorder.backing = backing
	// Default direct reader still routes through the error-injecting wrapper so that
	// setDirectGetErr works; with no configured error it behaves as the raw store.
	recorder.direct = &errorInjectingReader{Reader: backing, h: recorder}
	return recorder
}

// cachedClient returns a client whose Get can hide objects or fail, while List and all
// writes pass through to the shared backing tracker.
func (h *harness) cachedClient() client.Client {
	return interceptor.NewClient(h.backing, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			h.mu.Lock()
			hidden := h.hiddenGets[key]
			getErr := h.getErr
			h.mu.Unlock()
			if getErr != nil {
				return getErr
			}
			if hidden {
				return apierrors.NewNotFound(schema.GroupResource{Resource: "ipaddressclaims"}, key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
}

func (h *harness) reclaimer() *Reclaimer {
	return &Reclaimer{Client: h.cachedClient(), DirectReader: h.direct}
}

func (h *harness) hideCachedGet(key types.NamespacedName) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hiddenGets[key] = true
}

func (h *harness) setCachedGetErr(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.getErr = err
}

func (h *harness) setDirectGetErr(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.directGetErr = err
}

func (h *harness) recordDelete(key client.ObjectKey) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.deleteCalls = append(h.deleteCalls, key)
}

func (h *harness) deletes() []client.ObjectKey {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]client.ObjectKey, len(h.deleteCalls))
	copy(out, h.deleteCalls)
	return out
}

// survivingAddresses lists the IPAddresses still present in the authoritative store.
func (h *harness) survivingAddresses(ctx context.Context) ([]ipamv1.IPAddress, error) {
	list := &ipamv1.IPAddressList{}
	if err := h.backing.List(ctx, list, client.InNamespace(testNamespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// errorInjectingReader wraps the authoritative reader, optionally failing claim Gets to
// simulate a transient API error during the second confirmation.
type errorInjectingReader struct {
	client.Reader
	h *harness
}

func (e *errorInjectingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	e.h.mu.Lock()
	err := e.h.directGetErr
	e.h.mu.Unlock()
	if err != nil {
		if _, ok := obj.(*ipamv1.IPAddressClaim); ok {
			return err
		}
	}
	return e.Reader.Get(ctx, key, obj, opts...)
}

// vanishingAddressReader makes a specific IPAddress appear NotFound on direct reads,
// emulating the object being removed between the cached list and the confirmation read.
type vanishingAddressReader struct {
	client.Reader
	vanish client.ObjectKey
}

func (v *vanishingAddressReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*ipamv1.IPAddress); ok && key == v.vanish {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "ipaddresses"}, key.Name)
	}
	return v.Reader.Get(ctx, key, obj, opts...)
}

// Object builders used to construct fixtures.

func claim(uid types.UID, name string, addressRef string) *ipamv1.IPAddressClaim {
	c := &ipamv1.IPAddressClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			UID:       uid,
		},
	}
	if addressRef != "" {
		c.Status.AddressRef = ipamv1.IPAddressReference{Name: addressRef}
	}
	return c
}

// addressOpt customizes a built IPAddress.
type addressOpt func(*ipamv1.IPAddress)

func withClaimUID(uid types.UID) addressOpt {
	return func(a *ipamv1.IPAddress) {
		if a.Annotations == nil {
			a.Annotations = map[string]string{}
		}
		a.Annotations["ipam.cluster.x-k8s.io/claim-uid"] = string(uid)
	}
}

func withFinalizers(f ...string) addressOpt {
	return func(a *ipamv1.IPAddress) {
		a.Finalizers = append(a.Finalizers, f...)
	}
}

func withControllerOwner(uid types.UID, name string) addressOpt {
	return func(a *ipamv1.IPAddress) {
		controller := true
		a.OwnerReferences = append(a.OwnerReferences, metav1.OwnerReference{
			APIVersion: ipamv1.GroupVersion.String(),
			Kind:       "IPAddressClaim",
			Name:       name,
			UID:        uid,
			Controller: &controller,
		})
	}
}

func withDeletionTimestamp() addressOpt {
	return func(a *ipamv1.IPAddress) {
		now := metav1.Now()
		a.DeletionTimestamp = &now
	}
}

func address(name, ip string, opts ...addressOpt) *ipamv1.IPAddress {
	a := &ipamv1.IPAddress{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   testNamespace,
			Annotations: map[string]string{},
		},
		Spec: ipamv1.IPAddressSpec{
			ClaimRef: ipamv1.IPAddressClaimReference{Name: name},
			PoolRef: ipamv1.IPPoolReference{
				APIGroup: v1alpha2.GroupVersion.Group,
				Kind:     "InClusterIPPool",
				Name:     testPoolName,
			},
			Address: ip,
		},
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}
