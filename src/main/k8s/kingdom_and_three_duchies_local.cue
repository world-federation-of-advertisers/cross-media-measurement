// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// cue cmd dump src/main/k8s/kingdom_and_three_duchies_from_cue.cue >
// src/main/k8s/kingdom_and_three_duchies_from_cue.yaml

package k8s

objectSets: [
		fake_service,
		fake_pod,
		setup_job,
		kingdom.kingdom_service,
		kingdom.kingdom_pod,
		kingdom.kingdom_job,
] + [ for d in duchies for v in d {v}]

fake_service: "spanner-emulator": {
	apiVersion: "v1"
	kind:       "Service"
	metadata: {
		name: "spanner-emulator"
		labels: "app.kubernetes.io/name": #AppName
	}
	spec: {
		selector: app: "spanner-emulator-app"
		type: "NodePort"
		ports: [{
			name:       "grpc"
			port:       9010
			protocol:   "TCP"
			targetPort: 9010
		}, {
			name:       "http"
			port:       9020
			protocol:   "TCP"
			targetPort: 9020
		}]
	}
}

fake_service: "fake-storage-server": #GrpcService & {
	_name:   "fake-storage-server"
	_system: "testing"
}

fake_pod: "spanner-emulator-pod": {
	apiVersion: "v1"
	kind:       "Pod"
	metadata: {
		name: "spanner-emulator-pod"
		labels: {
			app:                      "spanner-emulator-app"
			"app.kubernetes.io/name": #AppName
		}
	}
	spec: containers: [{
		name:  "spanner-emulator-container"
		image: "gcr.io/cloud-spanner-emulator/emulator"
	}]
}

fake_pod: "fake-storage-server-pod": #ServerPod & {
	_name:   "fake-storage-server"
	_image:  "bazel/src/main/kotlin/org/wfanet/measurement/storage/filesystem:server_image"
	_system: "testing"
	_args: [
		"--debug-verbose-grpc-server-logging=true",
		"--port=8080",
	]
}

#Duchies: [
	{
		name: "a"
		key:  "057b22ef9c4e9626c22c13daed1363a1e6a5b309a930409f8d131f96ea2fa888"
	},
	{
		name: "b"
		key:  "31cc32e7cd53ff24f2b64ae8c531099af9867ebf5d9a659f742459947caa29b0"
	},
	{
		name: "c"
		key:  "338cce0306416b70e901436cb9eca5ac758e8ff41d7b58dabadf8726608ca6cc"
	},
]

#LocalDuchy: #Duchy & {
	_duchy_names: [ for d in #Duchies {d.name}]
	_spanner_schema_push_flags: [
		"--create-instance",
		"--emulator-host=" + (#Target & {name: "spanner-emulator"}).target,
		"--instance-config-id=spanner-emulator",
		"--instance-display-name=EmulatorInstance",
		"--instance-name=emulator-instance",
		"--instance-node-count=1",
		"--project-name=ads-open-measurement",
	]
	_spanner_flags: [
		"--spanner-emulator-host=" + (#Target & {name: "spanner-emulator"}).target,
		"--spanner-instance=emulator-instance",
		"--spanner-project=ads-open-measurement",
	]
	_blob_storage_flags: [
		"--forwarded-storage-service-target=" + (#Target & {name: "fake-storage-server"}).target,
	]
	_images: {
		"async-computation-control-server": "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/server:async_computation_control_server_image"
		"computation-control-server":       "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/server:forwarded_storage_computation_control_server_image"
		"herald-daemon":                    "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/daemon/herald:herald_daemon_image"
		"liquid-legions-v1-mill-daemon":    "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/daemon/mill/liquidlegionsv1:forwarded_storage_liquid_legions_v1_mill_daemon_image"
		"liquid-legions-v2-mill-daemon":    "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/daemon/mill/liquidlegionsv2:forwarded_storage_liquid_legions_v2_mill_daemon_image"
		"metric-values-storage-server":     "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/server:spanner_forwarded_storage_server_image"
		"publisher-data-server":            "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/server:publisher_data_server_image"
		"push-spanner-schema-container":    "bazel/src/main/kotlin/org/wfanet/measurement/tools:push_spanner_schema_image"
		"spanner-computations-server":      "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/server:spanner_computations_server_image"
	}
	_duchy_image_pull_policy: "Never"
	_verbose_grpc_logging:    "true"
}

duchies: {for d in #Duchies {"\(d.name)": #LocalDuchy & {_duchy: d}}}

kingdom: #Kingdom & {
	_duchy_ids: [ for d in #Duchies {"duchy-\(d.name)"}]
	_spanner_schema_push_flags: [
		"--create-instance",
		"--emulator-host=" + (#Target & {name: "spanner-emulator"}).target,
		"--instance-config-id=spanner-emulator",
		"--instance-display-name=EmulatorInstance",
		"--instance-name=emulator-instance",
		"--instance-node-count=1",
		"--project-name=ads-open-measurement",
	]
	_spanner_flags: [
		"--spanner-database=kingdom",
		"--spanner-emulator-host=" + (#Target & {name: "spanner-emulator"}).target,
		"--spanner-instance=emulator-instance",
		"--spanner-project=ads-open-measurement",
	]
	_images: {
		"push-spanner-schema-container": "bazel/src/main/kotlin/org/wfanet/measurement/tools:push_spanner_schema_image"
		"report-maker-daemon":           "bazel/src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/daemon:report_maker_daemon_image"
		"report-starter-daemon":         "bazel/src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/daemon:report_starter_daemon_image"
		"requisition-linker-daemon":     "bazel/src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/daemon:requisition_linker_daemon_image"
		"gcp-kingdom-data-server":       "bazel/src/main/kotlin/org/wfanet/measurement/kingdom/deploy/gcloud/server:gcp_kingdom_data_server_image"
		"global-computation-server":     "bazel/src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/server:global_computation_server_image"
		"requisition-server":            "bazel/src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/server:requisition_server_image"
		"system-requisition-server":     "bazel/src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/server:system_requisition_server_image"
	}
	_kingdom_image_pull_policy: "Never"
	_verbose_grpc_logging:      "true"
}

setup_job: "correctness-test-job": #CorrectnessTest & {
	_image:           "bazel/src/main/kotlin/org/wfanet/measurement/loadtest:filesystem_storage_correctness_runner_image"
	_imagePullPolicy: "Never"
	_args: [
		"--output-directory=correctness",
		"--spanner-database=kingdom",
		"--spanner-emulator-host=" + (#Target & {name: "spanner-emulator"}).target,
		"--spanner-instance=emulator-instance",
		"--spanner-project=ads-open-measurement",
	]
}
