// Copyright 2020 The Cross-Media Measurement Authors
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
		frontend_simulator,
		resource_setup_job,
		kingdom.kingdom_service,
		kingdom.kingdom_pod,
		kingdom.kingdom_job,
		kingdom.kingdom_internal_network_policies,
		duchy_network
] + [ for d in duchies for v in d {v}] + [ for d in edp_simulators {}]

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

#Edps: [
	{
		display_name:  "edp1"
		resource_name: "dataProviders/TBD"
	},
	{
		display_name:  "edp2"
		resource_name: "dataProviders/TBD"
	},
	{
		display_name:  "edp3"
		resource_name: "dataProviders/TBD"
	},
	{
		display_name:  "edp4"
		resource_name: "dataProviders/TBD"
	},
	{
		display_name:  "edp5"
		resource_name: "dataProviders/TBD"
	},
	{
		display_name:  "edp6"
		resource_name: "dataProviders/TBD"
	},
]

#Duchies: [
	{
		name:                   "aggregator"
		protocols_setup_config: #AggregatorProtocolsSetupConfig
		cs_cert_resource_name:  "duchies/aggregator/certificates/TBD"
	},
	{
		name:                   "worker1"
		protocols_setup_config: #NonAggregatorProtocolsSetupConfig
		cs_cert_resource_name:  "duchies/worker-1/certificates/TBD"
	},
	{
		name:                   "worker2"
		protocols_setup_config: #NonAggregatorProtocolsSetupConfig
		cs_cert_resource_name:  "duchies/worker-2/certificates/TBD"
	},
]

#LocalDuchy: #Duchy & {
	_spanner_schema_push_flags: [
		"--create-instance",
		"--emulator-host=" + (#Target & {name: "spanner-emulator"}).target,
		"--instance-config-id=spanner-emulator",
		"--instance-display-name=EmulatorInstance",
		"--instance-name=emulator-instance",
		"--instance-node-count=1",
		"--project-name=cross-media-measurement-system",
	]
	_spanner_flags: [
		"--spanner-emulator-host=" + (#Target & {name: "spanner-emulator"}).target,
		"--spanner-instance=emulator-instance",
		"--spanner-project=cross-media-measurement-system",
	]
	_blob_storage_flags: [
		"--forwarded-storage-service-target=" + (#Target & {name: "fake-storage-server"}).target,
	]
	_images: {
		"async-computation-control-server": "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/server:async_computation_control_server_image"
		"computation-control-server":       "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/server:forwarded_storage_computation_control_server_image"
		"herald-daemon":                    "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/daemon/herald:herald_daemon_image"
		"liquid-legions-v2-mill-daemon":    "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/daemon/mill/liquidlegionsv2:forwarded_storage_liquid_legions_v2_mill_daemon_image"
		"requisition-fulfillment-server":   "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/server:forwarded_storage_requisition_fulfillment_server_image"
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
		"--project-name=cross-media-measurement-system",
	]
	_spanner_flags: [
		"--spanner-database=kingdom",
		"--spanner-emulator-host=" + (#Target & {name: "spanner-emulator"}).target,
		"--spanner-instance=emulator-instance",
		"--spanner-project=cross-media-measurement-system",
	]
	_images: {
		"push-spanner-schema-container": "bazel/src/main/kotlin/org/wfanet/measurement/tools:push_spanner_schema_image"
		"gcp-kingdom-data-server":       "bazel/src/main/kotlin/org/wfanet/measurement/kingdom/deploy/gcloud/server:gcp_kingdom_data_server_image"
		"system-api-server":             "bazel/src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/server:system_api_server_image"
		"v2alpha-public-api-server":     "bazel/src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/server:v2alpha_public_api_server_image"
	}
	_kingdom_image_pull_policy: "Never"
	_verbose_grpc_logging:      "true"
}

frontend_simulator: "frontend_simulator": #FrontendSimulator & {
	_edp_display_names: [ for d in #Edps {d.display_name}]
	_mc_resource_name: "measurementConsumers/TBD"
	_image:            "bazel/src/main/kotlin/org/wfanet/measurement/loadtest/frontend:forwarded_storage_frontend_simulator_runner_image"
	_imagePullPolicy:  "Never"
	_blob_storage_flags: [
		"--forwarded-storage-service-target=" + (#Target & {name: "fake-storage-server"}).target,
	]
	_dependencies: ["v2alpha-public-api-server"]
}

resource_setup_job: "resource_setup_job": #ResourceSetup & {
	_edp_display_names: [ for d in #Edps {d.display_name}]
	_image:           "bazel/src/main/kotlin/org/wfanet/measurement/loadtest/resourcesetup:resource_setup_runner_image"
	_imagePullPolicy: "Never"
	_dependencies: ["v2alpha-public-api-server"]
}

edp_simulators: {
	for d in #Edps {
		"\(d.display_name)": #EdpSimulator & {
			_edp: d
			_blob_storage_flags: [
				"--forwarded-storage-service-target=" + (#Target & {name: "fake-storage-server"}).target,
			]
			_image:           "bazel/src/main/kotlin/org/wfanet/measurement/loadtest/dataprovider:forwarded_storage_edp_simulator_runner_image"
			_imagePullPolicy: "Never"
		}
	}
}
