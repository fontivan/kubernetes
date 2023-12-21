package sharedguaranteedresourceadd

import (
	"context"
	"fmt"
	"strings"
	"testing"

	configv1 "github.com/openshift/api/config/v1"
	configv1listers "github.com/openshift/client-go/config/listers/config/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/kubernetes/fake"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	coreapi "k8s.io/kubernetes/pkg/apis/core"
	kapi "k8s.io/kubernetes/pkg/apis/core"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
)

const (
	// workloadTypeManagement contains the type for the management workload
	workloadTypeManagement = "management"
	// managedCapacityLabel contains the name of the new management resource that will available under the node
	managedCapacityLabel = "management.workload.openshift.io/cores"
	// TODO: doc
	sharedCPUResource     = "openshift.io/shared-cpus"
	guaranteedCPUResource = "openshift.io/guaranteed-cpus"
)

func getMockSharedGuaranteedResourceAdd(namespace *corev1.Namespace, nodes []*corev1.Node, infra *configv1.Infrastructure) (*sharedGuaranteedResourceAdd, error) {
	m := &sharedGuaranteedResourceAdd{
		Handler:               admission.NewHandler(admission.Create),
		client:                &fake.Clientset{},
		nsLister:              fakeNamespaceLister(namespace),
		nsListerSynced:        func() bool { return true },
		nodeLister:            fakeNodeLister(nodes),
		nodeListSynced:        func() bool { return true },
		infraConfigLister:     fakeInfraConfigLister(infra),
		infraConfigListSynced: func() bool { return true },
	}
	if err := m.ValidateInitialization(); err != nil {
		return nil, err
	}

	return m, nil
}

func fakeNamespaceLister(ns *corev1.Namespace) corev1listers.NamespaceLister {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	_ = indexer.Add(ns)
	return corev1listers.NewNamespaceLister(indexer)
}

func fakeNodeLister(nodes []*corev1.Node) corev1listers.NodeLister {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, node := range nodes {
		_ = indexer.Add(node)
	}
	return corev1listers.NewNodeLister(indexer)
}

func fakeInfraConfigLister(infra *configv1.Infrastructure) configv1listers.InfrastructureLister {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if infra != nil {
		_ = indexer.Add(infra)
	}
	return configv1listers.NewInfrastructureLister(indexer)
}

func TestAdmit(t *testing.T) {
	tests := []struct {
		name                         string
		pod                          *kapi.Pod
		namespace                    *corev1.Namespace
		nodes                        []*corev1.Node
		infra                        *configv1.Infrastructure
		expectedSharedCPURequest     resource.Quantity
		expectedSharedCPULimit       resource.Quantity
		expectedGuaranteedCPURequest resource.Quantity
		expectedGuaranteedCPULimit   resource.Quantity
		expectedError                error
	}{
		{
			name:      "should skip static pod mutation",
			pod:       testManagedStaticPod("500m", "250m", "500Mi", "250Mi"),
			namespace: testManagedNamespace(),
			nodes:     []*corev1.Node{testNodeWithManagementResource()},
			infra:     testClusterSNOInfra(),
		},
		{
			name:      "should ignore best-effort pod with managed annotation",
			pod:       testManagedPod("", "", "", ""),
			namespace: testManagedNamespace(),
			nodes:     []*corev1.Node{testNodeWithManagementResource()},
			infra:     testClusterSNOInfra(),
		},
		{
			name:      "should not update container shared CPU requests/limits for the best-effort pod",
			pod:       testPod("", "", "", ""),
			namespace: testManagedNamespace(),
			nodes:     []*corev1.Node{testNodeWithManagementResource()},
			infra:     testClusterSNOInfra(),
		},
		{
			name:                     "should update container shared CPU requests/limits for the burstable pod",
			pod:                      testPod("500m", "250m", "1500Mi", "1250Mi"),
			expectedSharedCPURequest: resource.MustParse("250"),
			expectedSharedCPULimit:   resource.MustParse("250"),
			namespace:                testManagedNamespace(),
			nodes:                    []*corev1.Node{testNodeWithManagementResource()},
			infra:                    testClusterSNOInfra(),
		},
		{
			name:                     "should update container shared CPU requests/limits for the burstable pod with no limits",
			pod:                      testPod("", "250m", "", "1250Mi"),
			expectedSharedCPURequest: resource.MustParse("250"),
			expectedSharedCPULimit:   resource.MustParse("250"),
			namespace:                testManagedNamespace(),
			nodes:                    []*corev1.Node{testNodeWithManagementResource()},
			infra:                    testClusterSNOInfra(),
		},
		{
			name:                     "should update container shared CPU requests/limits for the burstable pod with only limits for cpu resource",
			pod:                      testPod("500m", "", "1500Mi", "1250Mi"),
			expectedSharedCPURequest: resource.MustParse("500"),
			expectedSharedCPULimit:   resource.MustParse("500"),
			namespace:                testManagedNamespace(),
			nodes:                    []*corev1.Node{testNodeWithManagementResource()},
			infra:                    testClusterSNOInfra(),
		},
		{
			name:                         "should update container guaranteed CPU requests/limits for the guaranteed pod with whole cpus",
			pod:                          testPod("2000m", "2000m", "1500Mi", "1500Mi"),
			expectedGuaranteedCPURequest: resource.MustParse("2000"),
			expectedGuaranteedCPULimit:   resource.MustParse("2000"),
			namespace:                    testManagedNamespace(),
			nodes:                        []*corev1.Node{testNodeWithManagementResource()},
			infra:                        testClusterSNOInfra(),
		},
		{
			name:                         "should update container guaranteed CPU requests/limits for the guaranteed pod with whole cpus and only limits",
			pod:                          testPod("2000m", "", "1500Mi", ""),
			expectedGuaranteedCPURequest: resource.MustParse("2000"),
			expectedGuaranteedCPULimit:   resource.MustParse("2000"),
			namespace:                    testManagedNamespace(),
			nodes:                        []*corev1.Node{testNodeWithManagementResource()},
			infra:                        testClusterSNOInfra(),
		},
		{
			name:                     "should update container shared CPU requests/limits for the guaranteed pod with fractional cpus",
			pod:                      testPod("500m", "500m", "1500Mi", "1500Mi"),
			expectedSharedCPURequest: resource.MustParse("500"),
			expectedSharedCPULimit:   resource.MustParse("500"),
			namespace:                testManagedNamespace(),
			nodes:                    []*corev1.Node{testNodeWithManagementResource()},
			infra:                    testClusterSNOInfra(),
		},
		{
			name:                     "should update container shared CPU requests/limits for the guaranteed pod with fractional cpus and only limits",
			pod:                      testPod("500m", "", "1500Mi", ""),
			expectedSharedCPURequest: resource.MustParse("500"),
			expectedSharedCPULimit:   resource.MustParse("500"),
			namespace:                testManagedNamespace(),
			nodes:                    []*corev1.Node{testNodeWithManagementResource()},
			infra:                    testClusterSNOInfra(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m, err := getMockSharedGuaranteedResourceAdd(test.namespace, test.nodes, test.infra)
			if err != nil {
				t.Fatalf("%s: failed to get mock sharedGuaranteedResourceAdd: %v", test.name, err)
			}

			test.pod.Namespace = test.namespace.Name

			attrs := admission.NewAttributesRecord(test.pod, nil, schema.GroupVersionKind{}, test.pod.Namespace, test.pod.Name, kapi.Resource("pods").WithVersion("version"), "", admission.Create, nil, false, fakeUser())
			err = m.Admit(context.TODO(), attrs, nil)
			if err != nil {
				if test.expectedError == nil {
					t.Fatalf("%s: admission controller returned error: %v", test.name, err)
				}

				if !strings.Contains(err.Error(), test.expectedError.Error()) {
					t.Fatalf("%s: the expected error %v, got %v", test.name, test.expectedError, err)
				}
			}

			if err == nil && test.expectedError != nil {
				t.Fatalf("%s: the expected error %v, got nil", test.name, test.expectedError)
			}

			resources := test.pod.Spec.InitContainers[0].Resources // only test one container

			if actual := resources.Requests[sharedCPUResource]; test.expectedSharedCPURequest.Cmp(actual) != 0 {
				t.Fatalf("%s: the shared CPU requests do not match; %v should be %v", test.name, actual, test.expectedSharedCPURequest)
			}

			if actual := resources.Limits[sharedCPUResource]; test.expectedSharedCPULimit.Cmp(actual) != 0 {
				t.Fatalf("%s: the shared CPU limits do not match; %v should be %v", test.name, actual, test.expectedSharedCPULimit)
			}

			if actual := resources.Requests[guaranteedCPUResource]; test.expectedGuaranteedCPURequest.Cmp(actual) != 0 {
				t.Fatalf("%s: the guaranteed CPU requests do not match; %v should be %v", test.name, actual, test.expectedGuaranteedCPURequest)
			}

			if actual := resources.Limits[guaranteedCPUResource]; test.expectedGuaranteedCPULimit.Cmp(actual) != 0 {
				t.Fatalf("%s: the guaranteed CPU limits do not match; %v should be %v", test.name, actual, test.expectedGuaranteedCPULimit)
			}

			resources = test.pod.Spec.Containers[0].Resources // only test one container

			if actual := resources.Requests[sharedCPUResource]; test.expectedSharedCPURequest.Cmp(actual) != 0 {
				t.Fatalf("%s: the shared CPU requests do not match; %v should be %v", test.name, actual, test.expectedSharedCPURequest)
			}

			if actual := resources.Limits[sharedCPUResource]; test.expectedSharedCPULimit.Cmp(actual) != 0 {
				t.Fatalf("%s: the shared CPU limits do not match; %v should be %v", test.name, actual, test.expectedSharedCPULimit)
			}

			if actual := resources.Requests[guaranteedCPUResource]; test.expectedGuaranteedCPURequest.Cmp(actual) != 0 {
				t.Fatalf("%s: the guaranteed CPU requests do not match; %v should be %v", test.name, actual, test.expectedGuaranteedCPURequest)
			}

			if actual := resources.Limits[guaranteedCPUResource]; test.expectedGuaranteedCPULimit.Cmp(actual) != 0 {
				t.Fatalf("%s: the guaranteed CPU limits do not match; %v should be %v", test.name, actual, test.expectedGuaranteedCPULimit)
			}
		})
	}
}

func TestGetPodQoSClass(t *testing.T) {
	tests := []struct {
		name             string
		pod              *kapi.Pod
		expectedQoSClass coreapi.PodQOSClass
	}{
		{
			name:             "should recognize best-effort pod",
			pod:              testManagedPod("", "", "", ""),
			expectedQoSClass: coreapi.PodQOSBestEffort,
		},
		{
			name:             "should recognize guaranteed pod",
			pod:              testManagedPod("100m", "100m", "100Mi", "100Mi"),
			expectedQoSClass: coreapi.PodQOSGuaranteed,
		},
		{
			name:             "should recognize guaranteed pod when CPU request equals to 0",
			pod:              testManagedPod("100m", "0", "100Mi", "100Mi"),
			expectedQoSClass: coreapi.PodQOSGuaranteed,
		},
		{
			name:             "should recognize burstable pod with only CPU limit",
			pod:              testManagedPod("100m", "", "", ""),
			expectedQoSClass: coreapi.PodQOSBurstable,
		},
		{
			name:             "should recognize burstable pod with only CPU request",
			pod:              testManagedPod("", "100m", "", ""),
			expectedQoSClass: coreapi.PodQOSBurstable,
		},
		{
			name:             "should recognize burstable pod with only memory limit",
			pod:              testManagedPod("", "", "100Mi", ""),
			expectedQoSClass: coreapi.PodQOSBurstable,
		},
		{
			name:             "should recognize burstable pod with only memory request",
			pod:              testManagedPod("", "", "", "100Mi"),
			expectedQoSClass: coreapi.PodQOSBurstable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			allContainers := append([]coreapi.Container{}, test.pod.Spec.InitContainers...)
			allContainers = append(allContainers, test.pod.Spec.Containers...)
			qosClass := getPodQoSClass(allContainers)
			if qosClass != test.expectedQoSClass {
				t.Fatalf("%s: pod has QoS class %s; should be %s", test.name, qosClass, test.expectedQoSClass)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name          string
		pod           *kapi.Pod
		namespace     *corev1.Namespace
		nodes         []*corev1.Node
		expectedError error
	}{
		{
			name:      "should skip static pod validation",
			pod:       testManagedStaticPod("500m", "250m", "500Mi", "250Mi"),
			namespace: testManagedNamespace(),
			nodes:     []*corev1.Node{testNodeWithManagementResource()},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m, err := getMockSharedGuaranteedResourceAdd(test.namespace, test.nodes, nil)
			if err != nil {
				t.Fatalf("%s: failed to get mock sharedGuaranteedResourceAdd: %v", test.name, err)
			}
			test.pod.Namespace = test.namespace.Name

			attrs := admission.NewAttributesRecord(test.pod, nil, schema.GroupVersionKind{}, test.pod.Namespace, test.pod.Name, kapi.Resource("pods").WithVersion("version"), "", admission.Create, nil, false, fakeUser())
			err = m.Validate(context.TODO(), attrs, nil)
			if err != nil {
				if test.expectedError == nil {
					t.Fatalf("%s: admission controller returned error: %v", test.name, err)
				}

				if !strings.Contains(err.Error(), test.expectedError.Error()) {
					t.Fatalf("%s: the expected error %v, got %v", test.name, test.expectedError, err)
				}
			}

			if err == nil && test.expectedError != nil {
				t.Fatalf("%s: the expected error %v, got nil", test.name, test.expectedError)
			}
		})
	}
}

func testPodWithManagedResource(cpuLimit, cpuRequest, memoryLimit, memoryRequest string) *kapi.Pod {
	pod := testPod(cpuLimit, cpuRequest, memoryLimit, memoryRequest)

	managedResourceName := fmt.Sprintf("%s.%s", workloadTypeManagement, containerWorkloadResourceSuffix)

	managedResourceQuantity := resource.MustParse("26")
	pod.Spec.Containers[0].Resources.Requests[kapi.ResourceName(managedResourceName)] = managedResourceQuantity
	return pod
}

func testManagedPodWithAnnotations(cpuLimit, cpuRequest, memoryLimit, memoryRequest string, annotations map[string]string) *kapi.Pod {
	pod := testManagedPod(cpuLimit, cpuRequest, memoryLimit, memoryRequest)
	pod.Annotations = annotations
	return pod
}

func testManagedStaticPod(cpuLimit, cpuRequest, memoryLimit, memoryRequest string) *kapi.Pod {
	pod := testManagedPod(cpuLimit, cpuRequest, memoryLimit, memoryRequest)
	pod.Annotations[kubetypes.ConfigSourceAnnotationKey] = kubetypes.FileSource
	return pod
}

func testManagedPod(cpuLimit, cpuRequest, memoryLimit, memoryRequest string) *kapi.Pod {
	pod := testPod(cpuLimit, cpuRequest, memoryLimit, memoryRequest)

	pod.Annotations = map[string]string{}
	for _, c := range pod.Spec.InitContainers {
		cpusetAnnotation := fmt.Sprintf("%s%s", containerResourcesAnnotationPrefix, c.Name)
		pod.Annotations[cpusetAnnotation] = `{"cpuset": "0-1"}`
	}
	for _, c := range pod.Spec.Containers {
		cpusetAnnotation := fmt.Sprintf("%s%s", containerResourcesAnnotationPrefix, c.Name)
		pod.Annotations[cpusetAnnotation] = `{"cpuset": "0-1"}`
	}

	managementWorkloadAnnotation := fmt.Sprintf("%s%s", podWorkloadTargetAnnotationPrefix, workloadTypeManagement)
	pod.Annotations = map[string]string{
		managementWorkloadAnnotation: fmt.Sprintf(`{"%s":"%s"}`, podWorkloadAnnotationEffect, workloadEffectPreferredDuringScheduling),
	}

	return pod
}

func testPod(cpuLimit, cpuRequest, memoryLimit, memoryRequest string) *kapi.Pod {
	pod := &kapi.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "test",
		},
		Spec: kapi.PodSpec{
			InitContainers: []kapi.Container{
				{
					Name: "initTest",
				},
			},
			Containers: []kapi.Container{
				{
					Name: "test",
				},
			},
		},
	}

	var limits kapi.ResourceList
	// we need this kind of statement to verify assignment to entry in nil map
	if cpuLimit != "" || memoryLimit != "" {
		limits = kapi.ResourceList{}
		if cpuLimit != "" {
			limits[kapi.ResourceCPU] = resource.MustParse(cpuLimit)
		}

		if memoryLimit != "" {
			limits[kapi.ResourceMemory] = resource.MustParse(memoryLimit)
		}

		pod.Spec.InitContainers[0].Resources.Limits = limits.DeepCopy()
		pod.Spec.Containers[0].Resources.Limits = limits.DeepCopy()
	}

	var requests kapi.ResourceList
	// we need this kind of statement to verify assignment to entry in nil map
	if cpuRequest != "" || memoryRequest != "" {
		requests = kapi.ResourceList{}
		if cpuRequest != "" {
			requests[kapi.ResourceCPU] = resource.MustParse(cpuRequest)
		}
		if memoryRequest != "" {
			requests[kapi.ResourceMemory] = resource.MustParse(memoryRequest)
		}

		pod.Spec.InitContainers[0].Resources.Requests = requests.DeepCopy()
		pod.Spec.Containers[0].Resources.Requests = requests.DeepCopy()
	}

	return pod
}

func fakeUser() user.Info {
	return &user.DefaultInfo{
		Name: "testuser",
	}
}

func testNamespace() *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "namespace",
		},
	}
}

func testManagedNamespace() *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "managed-namespace",
			Annotations: map[string]string{
				namespaceAllowedAnnotation: fmt.Sprintf("%s,test", workloadTypeManagement),
			},
		},
	}
}

func testNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node",
		},
	}
}

func testNodeWithManagementResource() *corev1.Node {
	q := resource.NewQuantity(16000, resource.DecimalSI)
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "managed-node",
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				managedCapacityLabel: *q,
			},
		},
	}
}

func testClusterInfraWithoutAnyStatusFields() *configv1.Infrastructure {
	return &configv1.Infrastructure{
		ObjectMeta: metav1.ObjectMeta{
			Name: infraClusterName,
		},
	}
}

func testClusterSNOInfra() *configv1.Infrastructure {
	return &configv1.Infrastructure{
		ObjectMeta: metav1.ObjectMeta{
			Name: infraClusterName,
		},
		Status: configv1.InfrastructureStatus{
			APIServerURL:           "test",
			ControlPlaneTopology:   configv1.SingleReplicaTopologyMode,
			InfrastructureTopology: configv1.SingleReplicaTopologyMode,
			CPUPartitioning:        configv1.CPUPartitioningAllNodes,
		},
	}
}

func testClusterInfraWithoutTopologyFields() *configv1.Infrastructure {
	infra := testClusterSNOInfra()
	infra.Status.ControlPlaneTopology = ""
	infra.Status.InfrastructureTopology = ""
	return infra
}
