package sharedguaranteedresourceadd

// The SharedGuaranteedResourceAdd admission plugin adds new pod container requests and limits
// for shared or guaranteed CPUs as follows:
// - If a pod has target.workload.openshift.io/management annotation in a namespace with the
//   workload.openshift.io/allowed management annotation, ignore this pod as it will be handled
//   by the managementcpusoverride admission hook.
// - If a pod has the Burstable or BestEffort QoS, add openshift.io/shared-cpus requests/limits
//   for each container, matching the CPU requests.
// - If a pod has the Guaranteed QoS:
//   - for any containers with whole CPU requests/limits, add openshift.io/guaranteed-cpus
//     requests/limits
//   - for any containers with fractional CPU requests/limits, add openshift.io/shared-cpus
//     requests/limits
//
// TODO: is there a better way to check whether the managementcpusoverride admission hook will handle the pod?
