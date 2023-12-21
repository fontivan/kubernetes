/*
Copyright 2023 The Kubernetes Authors.
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

package partition

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

type Isolated struct {
	Cpuset string
}

type PartitionedCpus struct {
	Isolated Isolated
}

var (
	PartitionPrefix         = "openshift.io/"
	partitionedCpusFilename = "/etc/kubernetes/openshift-partitioned-cpus"
	partitionedCpusEnabled  bool
	partitionedCpus         PartitionedCpus
	isolatedCpus            string
)

func GeneratePartitionName(partitionName string) v1.ResourceName {
	return v1.ResourceName(fmt.Sprintf("%v%v", PartitionPrefix, partitionName))
}

func PartitioningEnabled() bool {
	return partitionedCpusEnabled
}

func GetIsolatedCpuset() string {
	return isolatedCpus
}

func init() {
	readEnablementFile()
}

func readEnablementFile() {
	if _, err := os.Stat(partitionedCpusFilename); err == nil {
		if enablementFile, err := os.Open(partitionedCpusFilename); err == nil {
			byteValue, _ := ioutil.ReadAll(enablementFile)
			json.Unmarshal(byteValue, &partitionedCpus)
			isolatedCpus = partitionedCpus.Isolated.Cpuset
			partitionedCpusEnabled = true
			klog.InfoS("partitionedCpus", "partitionedCpus", isolatedCpus)
			enablementFile.Close()
		}
	}
}
