package sharedguaranteedresourceadd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	configv1 "github.com/openshift/api/config/v1"
	configv1informer "github.com/openshift/client-go/config/informers/externalversions/config/v1"
	configv1listers "github.com/openshift/client-go/config/listers/config/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	coreapi "k8s.io/kubernetes/pkg/apis/core"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
)

const (
	PluginName = "autoscaling.openshift.io/SharedGuaranteedResourceAdd"
	// timeToWaitForCacheSync contains the time how long to wait for caches to be synchronize
	timeToWaitForCacheSync = 10 * time.Second
	// containerWorkloadResourceSuffix contains the suffix for the container workload resource
	containerWorkloadResourceSuffix = "workload.openshift.io/cores"
	// podWorkloadTargetAnnotationPrefix contains the prefix for the pod workload target annotation
	podWorkloadTargetAnnotationPrefix = "target.workload.openshift.io/"
	// podWorkloadAnnotationEffect contains the effect key for the workload annotation value
	podWorkloadAnnotationEffect = "effect"
	// workloadEffectPreferredDuringScheduling contains the PreferredDuringScheduling effect value
	workloadEffectPreferredDuringScheduling = "PreferredDuringScheduling"
	// containerResourcesAnnotationPrefix contains resource annotation prefix that will be used by CRI-O to set cpu shares
	containerResourcesAnnotationPrefix = "resources.workload.openshift.io/"
	// containerResourcesAnnotationValueKeyCPUShares contains resource annotation value cpushares key
	containerResourcesAnnotationValueKeyCPUShares = "cpushares"
	// namespaceAllowedAnnotation contains the namespace allowed annotation key
	namespaceAllowedAnnotation = "workload.openshift.io/allowed"
	// workloadAdmissionWarning contains the admission warning annotation key
	workloadAdmissionWarning = "workload.openshift.io/warning"
	// infraClusterName contains the name of the cluster infrastructure resource
	infraClusterName = "cluster"
	// debugSourceResourceAnnotation contains the debug annotation that refers to the pod resource
	debugSourceResourceAnnotation = "debug.openshift.io/source-resource"
	// TODO: document
	openshiftResourcePrefix = "openshift.io"
	sharedCPUsResource      = "shared-cpus"
	guaranteedCPUsResource  = "guaranteed-cpus"
)

var _ = initializer.WantsExternalKubeInformerFactory(&sharedGuaranteedResourceAdd{})
var _ = initializer.WantsExternalKubeClientSet(&sharedGuaranteedResourceAdd{})
var _ = admission.MutationInterface(&sharedGuaranteedResourceAdd{})
var _ = admission.ValidationInterface(&sharedGuaranteedResourceAdd{})
var _ = WantsInfraInformer(&sharedGuaranteedResourceAdd{})

func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName,
		func(config io.Reader) (admission.Interface, error) {
			return &sharedGuaranteedResourceAdd{
				Handler: admission.NewHandler(admission.Create),
			}, nil
		})
}

// sharedGuaranteedResourceAdd presents admission plugin that ...
// TODO: duplicate docs here?
type sharedGuaranteedResourceAdd struct {
	*admission.Handler
	client                kubernetes.Interface
	nsLister              corev1listers.NamespaceLister
	nsListerSynced        func() bool
	nodeLister            corev1listers.NodeLister
	nodeListSynced        func() bool
	infraConfigLister     configv1listers.InfrastructureLister
	infraConfigListSynced func() bool
}

func (a *sharedGuaranteedResourceAdd) SetExternalKubeInformerFactory(kubeInformers informers.SharedInformerFactory) {
	// Bart: are all these needed?
	a.nsLister = kubeInformers.Core().V1().Namespaces().Lister()
	a.nsListerSynced = kubeInformers.Core().V1().Namespaces().Informer().HasSynced
	a.nodeLister = kubeInformers.Core().V1().Nodes().Lister()
	a.nodeListSynced = kubeInformers.Core().V1().Nodes().Informer().HasSynced
}

// SetExternalKubeClientSet implements the WantsExternalKubeClientSet interface.
func (a *sharedGuaranteedResourceAdd) SetExternalKubeClientSet(client kubernetes.Interface) {
	a.client = client
}

func (a *sharedGuaranteedResourceAdd) SetInfraInformer(informer configv1informer.InfrastructureInformer) {
	a.infraConfigLister = informer.Lister()
	a.infraConfigListSynced = informer.Informer().HasSynced
}

func (a *sharedGuaranteedResourceAdd) ValidateInitialization() error {
	if a.client == nil {
		return fmt.Errorf("%s plugin needs a kubernetes client", PluginName)
	}
	if a.nsLister == nil {
		return fmt.Errorf("%s did not get a namespace lister", PluginName)
	}
	if a.nsListerSynced == nil {
		return fmt.Errorf("%s plugin needs a namespace lister synced", PluginName)
	}
	if a.nodeLister == nil {
		return fmt.Errorf("%s did not get a node lister", PluginName)
	}
	if a.nodeListSynced == nil {
		return fmt.Errorf("%s plugin needs a node lister synced", PluginName)
	}
	if a.infraConfigLister == nil {
		return fmt.Errorf("%s did not get a config infrastructure lister", PluginName)
	}
	if a.infraConfigListSynced == nil {
		return fmt.Errorf("%s plugin needs a config infrastructure lister synced", PluginName)
	}
	return nil
}

func (a *sharedGuaranteedResourceAdd) Admit(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces) error {
	if attr.GetResource().GroupResource() != coreapi.Resource("pods") || attr.GetSubresource() != "" {
		return nil
	}

	pod, ok := attr.GetObject().(*coreapi.Pod)
	if !ok {
		return admission.NewForbidden(attr, fmt.Errorf("unexpected object: %#v", attr.GetObject()))
	}

	// do not mutate mirror pods at all
	if isStaticPod(pod.Annotations) {
		return nil
	}

	podAnnotations := map[string]string{}
	for k, v := range pod.Annotations {
		podAnnotations[k] = v
	}

	workloadType, err := getWorkloadType(podAnnotations)
	if err != nil {
		invalidError := getPodInvalidWorkloadAnnotationError(podAnnotations, err.Error())
		return errors.NewInvalid(coreapi.Kind("Pod"), pod.Name, field.ErrorList{invalidError})
	}

	// workload annotation is specified under the pod so do nothing - it means the
	// autoscaling.openshift.io/ManagementCPUsOverride plugin has already mutated the pod so it will
	// run on the reserved CPUs
	if len(workloadType) != 0 {
		return nil
	}

	if !a.waitForSyncedStore(time.After(timeToWaitForCacheSync)) {
		return admission.NewForbidden(attr, fmt.Errorf("%s node or namespace or infra config cache not synchronized", PluginName))
	}

	nodes, err := a.nodeLister.List(labels.Everything())
	if err != nil {
		return admission.NewForbidden(attr, err) // can happen due to informer latency
	}

	// we need to have nodes under the cluster to decide if the CPU partitioning is enabled
	if len(nodes) == 0 {
		return admission.NewForbidden(attr, fmt.Errorf("%s the cluster does not have any nodes", PluginName))
	}

	clusterInfra, err := a.infraConfigLister.Get(infraClusterName)
	if err != nil {
		return admission.NewForbidden(attr, err) // can happen due to informer latency
	}

	// the infrastructure status is empty, so we can not decide the cluster type
	if reflect.DeepEqual(clusterInfra.Status, configv1.InfrastructureStatus{}) {
		return admission.NewForbidden(attr, fmt.Errorf("%s infrastructure resource has empty status", PluginName))
	}

	// Check if we are in CPU Partitioning mode for AllNodes. This is a prerequisite for adding the shared/guaranteed
	// extended resources to the pods.
	if !isCPUPartitioning(clusterInfra.Status, nodes, workloadType) {
		return nil
	}

	allContainers := append([]coreapi.Container{}, pod.Spec.InitContainers...)
	allContainers = append(allContainers, pod.Spec.Containers...)
	podQoSClass := getPodQoSClass(allContainers)

	// Add shared and/or guaranteed extended resources to each container in the pod
	addPodResources(pod, podQoSClass)

	return nil
}

func isCPUPartitioning(infraStatus configv1.InfrastructureStatus, nodes []*corev1.Node, workloadType string) bool {
	// If status is not for CPU partitioning and we're single node we also check nodes to support upgrade event
	// TODO: This should not be needed after 4.13 as all clusters after should have this feature on at install time, or updated by migration in NTO.
	if infraStatus.CPUPartitioning != configv1.CPUPartitioningAllNodes && infraStatus.ControlPlaneTopology == configv1.SingleReplicaTopologyMode {
		managedResource := fmt.Sprintf("%s.%s", workloadType, containerWorkloadResourceSuffix)
		for _, node := range nodes {
			// We only expect a single node to exist, so we return on first hit
			if _, ok := node.Status.Allocatable[corev1.ResourceName(managedResource)]; ok {
				return true
			}
		}
	}
	return infraStatus.CPUPartitioning == configv1.CPUPartitioningAllNodes
}

func (a *sharedGuaranteedResourceAdd) getPodNamespace(attr admission.Attributes) (*corev1.Namespace, error) {
	ns, err := a.nsLister.Get(attr.GetNamespace())
	if err == nil {
		return ns, nil
	}

	if !errors.IsNotFound(err) {
		return nil, admission.NewForbidden(attr, err)
	}

	// in case of latency in our caches, make a call direct to storage to verify that it truly exists or not
	ns, err = a.client.CoreV1().Namespaces().Get(context.TODO(), attr.GetNamespace(), metav1.GetOptions{})
	if err == nil {
		return ns, nil
	}

	if !errors.IsNotFound(err) {
		return nil, admission.NewForbidden(attr, err)
	}

	return nil, err
}

func (a *sharedGuaranteedResourceAdd) waitForSyncedStore(timeout <-chan time.Time) bool {
	for !a.nsListerSynced() || !a.nodeListSynced() || !a.infraConfigListSynced() {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-timeout:
			return a.nsListerSynced() && a.nodeListSynced() && a.infraConfigListSynced()
		}
	}

	return true
}

func addPodResources(pod *coreapi.Pod, class coreapi.PodQOSClass) {
	// add init containers resources
	addContainersResources(pod.Spec.InitContainers, class)

	// add app containers resources
	addContainersResources(pod.Spec.Containers, class)
}

func addContainersResources(containers []coreapi.Container, podQoSClass coreapi.PodQOSClass) {
	for i := range containers {
		c := &containers[i]

		if podQoSClass == coreapi.PodQOSGuaranteed {
			// The request/limit must match so just use the limit
			cpuLimit := c.Resources.Limits[coreapi.ResourceCPU]

			// check for whole cpus...
			if cpuLimit.Value()*1000 == cpuLimit.MilliValue() {
				// Add guaranteed-cpus requests/limits
				cpuLimitInMilli := cpuLimit.MilliValue()
				guaranteedCPURequest := resource.NewMilliQuantity(cpuLimitInMilli*1000, cpuLimit.Format)
				guaranteedCPUResource := fmt.Sprintf("%s/%s", openshiftResourcePrefix, guaranteedCPUsResource)
				// Create the requests if this container did not have them
				if c.Resources.Requests == nil {
					c.Resources.Requests = make(coreapi.ResourceList)
				}
				c.Resources.Requests[coreapi.ResourceName(guaranteedCPUResource)] = *guaranteedCPURequest
				c.Resources.Limits[coreapi.ResourceName(guaranteedCPUResource)] = *guaranteedCPURequest
				continue
			} else {
				// Add shared-cpus requests/limits
				cpuLimitInMilli := cpuLimit.MilliValue()
				sharedCPURequest := resource.NewMilliQuantity(cpuLimitInMilli*1000, cpuLimit.Format)
				sharedCPUResource := fmt.Sprintf("%s/%s", openshiftResourcePrefix, sharedCPUsResource)
				// Create the requests if this container did not have them
				if c.Resources.Requests == nil {
					c.Resources.Requests = make(coreapi.ResourceList)
				}
				c.Resources.Requests[coreapi.ResourceName(sharedCPUResource)] = *sharedCPURequest
				c.Resources.Limits[coreapi.ResourceName(sharedCPUResource)] = *sharedCPURequest
				continue
			}
		} else {
			// Add shared-cpus requests/limits if necessary
			cpuRequest, cpuRequestExists := c.Resources.Requests[coreapi.ResourceCPU]
			if cpuRequestExists {
				// If a request exists, match it.
				cpuRequestInMilli := cpuRequest.MilliValue()
				sharedCPURequest := resource.NewMilliQuantity(cpuRequestInMilli*1000, cpuRequest.Format)
				sharedCPUResource := fmt.Sprintf("%s/%s", openshiftResourcePrefix, sharedCPUsResource)
				c.Resources.Requests[coreapi.ResourceName(sharedCPUResource)] = *sharedCPURequest
				// Create the limits if this container did not have them
				if c.Resources.Limits == nil {
					c.Resources.Limits = make(coreapi.ResourceList)
				}
				c.Resources.Limits[coreapi.ResourceName(sharedCPUResource)] = *sharedCPURequest
				continue
			}

			cpuLimit, cpuLimitExists := c.Resources.Limits[coreapi.ResourceCPU]
			if cpuLimitExists {
				// If only a limit exists, it is implied that the request is the same, so match it.
				cpuLimitInMilli := cpuLimit.MilliValue()
				sharedCPURequest := resource.NewMilliQuantity(cpuLimitInMilli*1000, cpuRequest.Format)
				sharedCPUResource := fmt.Sprintf("%s/%s", openshiftResourcePrefix, sharedCPUsResource)
				c.Resources.Requests[coreapi.ResourceName(sharedCPUResource)] = *sharedCPURequest
				c.Resources.Limits[coreapi.ResourceName(sharedCPUResource)] = *sharedCPURequest
				continue
			}
		}
	}
}

func isGuaranteed(containers []coreapi.Container) bool {
	for _, c := range containers {
		// only memory and CPU resources are relevant to decide pod QoS class
		for _, r := range []coreapi.ResourceName{coreapi.ResourceMemory, coreapi.ResourceCPU} {
			limit := c.Resources.Limits[r]
			request, requestExist := c.Resources.Requests[r]

			if limit.IsZero() {
				return false
			}

			if !requestExist {
				continue
			}

			// it some corner case, when you set CPU request to 0 the k8s will change it to the value
			// specified under the limit
			if r == coreapi.ResourceCPU && request.IsZero() {
				continue
			}

			if !limit.Equal(request) {
				return false
			}
		}
	}

	return true
}

func isBestEffort(containers []coreapi.Container) bool {
	for _, c := range containers {
		// only memory and CPU resources are relevant to decide pod QoS class
		for _, r := range []coreapi.ResourceName{coreapi.ResourceMemory, coreapi.ResourceCPU} {
			limit := c.Resources.Limits[r]
			request := c.Resources.Requests[r]

			if !limit.IsZero() || !request.IsZero() {
				return false
			}
		}
	}

	return true
}

// TODO: Why is this here??? Is it because the standard function (https://github.com/openshift/kubernetes/blob/master/pkg/apis/core/helper/qos/qos.go)
// relies on both the requests/limits being set? In cases where the pod only has limits, is the matching requests being
// added after admission?
func getPodQoSClass(containers []coreapi.Container) coreapi.PodQOSClass {
	if isGuaranteed(containers) {
		return coreapi.PodQOSGuaranteed
	}

	if isBestEffort(containers) {
		return coreapi.PodQOSBestEffort
	}

	return coreapi.PodQOSBurstable
}

func podHasBothCPULimitAndRequest(containers []coreapi.Container) bool {
	for _, c := range containers {
		_, cpuRequestExists := c.Resources.Requests[coreapi.ResourceCPU]
		_, cpuLimitExists := c.Resources.Limits[coreapi.ResourceCPU]

		if cpuRequestExists && cpuLimitExists {
			return true
		}
	}

	return false
}

func doesNamespaceAllowWorkloadType(annotations map[string]string, workloadType string) bool {
	v, found := annotations[namespaceAllowedAnnotation]
	if !found {
		return false
	}

	for _, t := range strings.Split(v, ",") {
		if workloadType == t {
			return true
		}
	}

	return false
}

func getWorkloadType(annotations map[string]string) (string, error) {
	var workloadAnnotationsKeys []string
	for k := range annotations {
		if strings.HasPrefix(k, podWorkloadTargetAnnotationPrefix) {
			workloadAnnotationsKeys = append(workloadAnnotationsKeys, k)
		}
	}

	// no workload annotation is specified under the pod
	if len(workloadAnnotationsKeys) == 0 {
		return "", nil
	}

	// more than one workload annotation exists under the pod and we do not support different workload types
	// under the same pod
	if len(workloadAnnotationsKeys) > 1 {
		return "", fmt.Errorf("the pod can not have more than one workload annotations")
	}

	workloadType := strings.TrimPrefix(workloadAnnotationsKeys[0], podWorkloadTargetAnnotationPrefix)
	if len(workloadType) == 0 {
		return "", fmt.Errorf("the workload annotation key should have format %s<workload_type>, when <workload_type> is non empty string", podWorkloadTargetAnnotationPrefix)
	}

	return workloadType, nil
}

func getWorkloadAnnotationEffect(workloadAnnotationKey string) (string, error) {
	managementAnnotationValue := map[string]string{}
	if err := json.Unmarshal([]byte(workloadAnnotationKey), &managementAnnotationValue); err != nil {
		return "", fmt.Errorf("failed to parse %q annotation value: %v", workloadAnnotationKey, err)
	}

	if len(managementAnnotationValue) > 1 {
		return "", fmt.Errorf("the workload annotation value %q has more than one key", managementAnnotationValue)
	}

	effect, ok := managementAnnotationValue[podWorkloadAnnotationEffect]
	if !ok {
		return "", fmt.Errorf("the workload annotation value %q does not have %q key", managementAnnotationValue, podWorkloadAnnotationEffect)
	}
	return effect, nil
}

func stripResourcesAnnotations(annotations map[string]string) {
	for k := range annotations {
		if strings.HasPrefix(k, containerResourcesAnnotationPrefix) {
			delete(annotations, k)
		}
	}
}

func stripWorkloadAnnotations(annotations map[string]string) {
	for k := range annotations {
		if strings.HasPrefix(k, podWorkloadTargetAnnotationPrefix) {
			delete(annotations, k)
		}
	}
}

func addWorkloadAnnotations(annotations map[string]string, workloadType string) {
	if annotations == nil {
		annotations = map[string]string{}
	}

	workloadAnnotation := fmt.Sprintf("%s%s", podWorkloadTargetAnnotationPrefix, workloadType)
	annotations[workloadAnnotation] = fmt.Sprintf(`{"%s":"%s"}`, podWorkloadAnnotationEffect, workloadEffectPreferredDuringScheduling)
}

func (a *sharedGuaranteedResourceAdd) Validate(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces) (err error) {
	if attr.GetResource().GroupResource() != coreapi.Resource("pods") || attr.GetSubresource() != "" {
		return nil
	}

	pod, ok := attr.GetObject().(*coreapi.Pod)
	if !ok {
		return admission.NewForbidden(attr, fmt.Errorf("unexpected object: %#v", attr.GetObject()))
	}

	// do not validate mirror pods at all
	if isStaticPod(pod.Annotations) {
		return nil
	}

	// TODO: Not sure if there is anything we should be validating?
	return nil
}

func getPodInvalidWorkloadAnnotationError(annotations map[string]string, message string) *field.Error {
	return field.Invalid(field.NewPath("metadata.Annotations"), annotations, message)
}

// isStaticPod returns true if the pod is a static pod.
func isStaticPod(annotations map[string]string) bool {
	source, ok := annotations[kubetypes.ConfigSourceAnnotationKey]
	return ok && source != kubetypes.ApiserverSource
}

func isDebugPod(annotations map[string]string) bool {
	_, ok := annotations[debugSourceResourceAnnotation]
	return ok
}
