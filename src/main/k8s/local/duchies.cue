// Copyright 2021 The Cross-Media Measurement Authors
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

package k8s

_secret_name:          string @tag("secret_name")
_aggregator_cert_name: string @tag("aggregator_cert_name")
_worker1_cert_name:    string @tag("worker1_cert_name")
_worker2_cert_name:    string @tag("worker2_cert_name")

#KingdomSystemApiTarget: (#Target & {name: "system-api-server"}).target
#SpannerEmulatorHost:    (#Target & {name: "spanner-emulator"}).target
#DuchyConfig: {
	let duchyName = name
	name:                            string
	protocolsSetupConfig:            string
	certificateResourceName:         string
	computationControlServiceTarget: (#Target & {name: "\(duchyName)-computation-control-server"}).target
}
_duchyConfigs: [Name=_]: #DuchyConfig & {
	name: Name
}
_duchyConfigs: {
	"aggregator": {
		protocolsSetupConfig:    "aggregator_protocols_setup_config.textproto"
		certificateResourceName: _aggregator_cert_name
	}
	"worker1": {
		protocolsSetupConfig:    "non_aggregator_protocols_setup_config.textproto"
		certificateResourceName: _worker1_cert_name
	}
	"worker2": {
		protocolsSetupConfig:    "non_aggregator_protocols_setup_config.textproto"
		certificateResourceName: _worker2_cert_name
	}
}

objectSets: [ for duchy in duchies for objectSet in duchy {objectSet}]

_computationControlTargets: {
	for name, duchyConfig in _duchyConfigs {
		"\(name)": duchyConfig.computationControlServiceTarget
	}
}

duchies: [ for duchyConfig in _duchyConfigs {
	#Duchy & {
		_duchy: {
			name:                   duchyConfig.name
			protocols_setup_config: duchyConfig.protocolsSetupConfig
			cs_cert_resource_name:  duchyConfig.certificateResourceName
		}
		_duchy_secret_name:           _secret_name
		_computation_control_targets: _computationControlTargets
		_kingdom_system_api_target:   #KingdomSystemApiTarget
		_blob_storage_flags: [
			"--forwarded-storage-service-target=" + (#Target & {name: "fake-storage-server"}).target,
			"--forwarded-storage-cert-host=localhost",
		]
		_images: {
			"async-computation-control-server": "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/server:async_computation_control_server_image"
			"computation-control-server":       "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/server:forwarded_storage_computation_control_server_image"
			"herald-daemon":                    "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/daemon/herald:herald_daemon_image"
			"liquid-legions-v2-mill-daemon":    "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/daemon/mill/liquidlegionsv2:forwarded_storage_liquid_legions_v2_mill_daemon_image"
			"requisition-fulfillment-server":   "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/server:forwarded_storage_requisition_fulfillment_server_image"
			"spanner-computations-server":      "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/server:forwarded_storage_spanner_computations_server_image"
			"update-duchy-schema":              "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/spanner/tools:update_schema_image"
			"computations-cleaner":             "bazel/src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/job:computations_cleaner_image"
		}
		_duchy_image_pull_policy: "Never"
		_verbose_grpc_logging:    "true"
	}
}]
