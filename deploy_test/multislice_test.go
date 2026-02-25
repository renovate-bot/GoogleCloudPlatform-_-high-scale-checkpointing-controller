// Copyright 2026 Google LLC
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

package deploy_test

import (
	"context"
	"strconv"
	"testing"

	"gotest.tools/v3/assert"
	corev1 "k8s.io/api/core/v1"
	jobsetv1alpha "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

func createMultisliceJobset(ctx context.Context, t *testing.T, name string, selector map[string]string) *jobsetv1alpha.JobSet {
	t.Helper()
	params := getScaleTestParams(t)
	if params == nil {
		t.Fatalf("no multislice parameters")
	}
	nodes := params.slices * params.nodesPerSlice
	jobset := createMultitierJobsetSpec(name, selector, params.slices, params.nodesPerSlice)
	objmeta := &jobset.Spec.ReplicatedJobs[0].Template.Spec.Template.ObjectMeta
	if objmeta.Annotations == nil {
		objmeta.Annotations = map[string]string{}
	}
	objmeta.Annotations["gke-gcsfuse/volumes"] = "true"
	jobset.Spec.FailurePolicy.MaxRestarts = 0
	jobset.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Volumes = append(jobset.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "backup",
		VolumeSource: corev1.VolumeSource{
			CSI: &corev1.CSIVolumeSource{
				Driver: "gcsfuse.csi.storage.gke.io",
				VolumeAttributes: map[string]string{
					"bucketName": getEmulatorBucket(t),
				},
			},
		},
	})
	jobset.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Containers[0] = corev1.Container{
		Name:  "fake-ml-model",
		Image: params.workerImage,
		Command: []string{
			"python3",
			"fake-ml-model-entrypoint.py",
			"--no-use_redis",
		},
		Env: []corev1.EnvVar{
			{
				Name: "POD_INDEX",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.labels['batch.kubernetes.io/job-completion-index']",
					},
				},
			},
			{
				Name: "JOB_INDEX",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.labels['jobset.sigs.k8s.io/job-index']",
					},
				},
			},
			{
				Name:  "NODE_COUNT",
				Value: strconv.Itoa(nodes),
			},
			{
				Name:  "WORKER_COUNT",
				Value: "1",
			},
			{
				Name:  "NODE_COUNT_PER_JOB",
				Value: strconv.Itoa(params.nodesPerSlice),
			},
			{
				Name:  "NO_DEBUG_BACKUP",
				Value: "true",
			},
			{
				Name:  "JOB_NAME",
				Value: "dima",
			},
			{
				Name:  "WORKLOAD_UUID",
				Value: name,
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				MountPath: "/app/local",
				Name:      "checkpoint",
			},
			{
				MountPath: "/app/backup",
				Name:      "backup",
			},
		},
	}
	err := CRClient.Create(ctx, jobset)
	assert.NilError(t, err, "cannot create jobset %s", name)
	t.Logf("created jobset %s/%s", jobset.GetNamespace(), jobset.GetName())
	return jobset
}

func TestMultisliceEmulated(t *testing.T) {
	params := getScaleTestParams(t)
	if params == nil {
		t.Skip("No multislice parameters")
	}
	ctx := context.Background()
	// The gcs account used for the emulated jobset is in the default
	// namespace; it needs to be set up bucket reads which is done
	// manually. Probably this can be done via gcloud instead and a
	// per-namespace k8s SA can be created instead.
	cleanup := deployMultitier(ctx, t, useDefaultNamespace{}, ramdiskSize("50Mi"))
	defer cleanup()

	uuid := "multislice"

	sliceOpts := []NodePoolOption{MachineType("n2-standard-4"), DiskSizeGb(50)}

	initializeTestSlices(ctx, t, params.slices, params.nodesPerSlice, sliceOpts...)

	selector := getMultitierSelector(ctx, t)

	setEmulatorConfig(ctx, t, uuid, 0, 4, waitForDisruption)

	jobset := createMultisliceJobset(ctx, t, uuid, selector)
	defer cleanupJobset(ctx, t, jobset.GetNamespace(), jobset.GetName())

	t.Log("jobset created, waiting to disrupt")
	assert.Assert(t, waitForDisruptionReady(ctx, t, uuid, 4))
	assert.Assert(t, clearDistruptionFiles(ctx, t, uuid))
	t.Log("starting disruption")
	pools := getTestSlices(ctx, t, sliceOpts...)
	for pool := range pools {
		deleteNodePool(ctx, pool)
		break
	}
	waitForFailedJobset(ctx, t, jobset)
	assert.NilError(t, CRClient.Delete(ctx, jobset))
	t.Log("detected failed jobset, recreating nodes")

	// This will recreate the missing pool
	initializeTestSlices(ctx, t, params.slices, params.nodesPerSlice, sliceOpts...)

	t.Log("creating new emulated and jobset")
	setEmulatorConfig(ctx, t, uuid, 4, 7)

	jobset = createMultisliceJobset(ctx, t, uuid, selector)

	t.Log("waiting for second run to succeed")
	waitForJobsetFinished(ctx, t, jobset)
}
