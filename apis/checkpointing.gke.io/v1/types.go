// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CheckpointConfiguration is a specification for a CheckpointConfiguration resource.
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
type CheckpointConfiguration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              CheckpointConfigurationSpec `json:"spec"`

	// +optional
	Status CheckpointConfigurationStatus `json:"status,omitempty"`
}

// CheckpointConfigurationSpec is the spec for a CheckpointConfiguration resource.
type CheckpointConfigurationSpec struct {
	CloudStorageBucketName      string              `json:"cloudStorageBucketName,omitempty"`
	EnableExternalJaxProcessIds bool                `json:"enableExternalJaxProcessIds,omitempty"`
	NodeSelector                map[string]string   `json:"nodeSelector,omitempty"`
	Tolerations                 []corev1.Toleration `json:"tolerations,omitempty"`
	InMemoryVolumeSize          string              `json:"inMemoryVolumeSize,omitempty"`
	CsiEphemeralLimit           string              `json:"csiEphemeralLimit,omitempty"`
	ReplicationOptions          []string            `json:"replicationOptions,omitempty"`

	// GcsFuseMountOptions is a comma-separated list of mount options. This is
	// copied directly into the mountOptions for the driver GCS volume.
	GcsFuseMountOptions []string `json:"gcsFuseMountOptions,omitempty"`
}

// CheckpointConfigurationSpeceList is a list of CheckpointConfiguration resources.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CheckpointConfigurationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CheckpointConfiguration `json:"items"`
}

// CheckpointConfigurationStatus represents the current status of the configuration.
type CheckpointConfigurationStatus struct {
	// CurrentDriverPods is like the daemonset's CurrentNumberScheduled status.
	CurrentDriverPods int32 `json:"currentDriverPods"`

	// MisscheduledDriverPods is like the daemonset's NumberMisscheduled status.
	MisscheduledDriverPods int32 `json:"misscheduledDriverPods"`

	// DesiredDriverPods is like the daemonset's DesiredNumberScheduled status.
	DesiredDriverPods int32 `json:"desiredDriverPods"`

	// ReadyDriverPods is like the daemonset's NumberReady status.
	ReadyDriverPods int32 `json:"readyDriverPods"`
}
