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

package k8s

import ("strings")

#Duchy: {
	_duchy: {name: string, key: string}
	_duchy_names: [...string]
	_spanner_schema_push_flags: [...string]
	_spanner_flags: [...string]
	_blob_storage_flags: [...string]
	_verbose_grpc_logging: "true" | "false"

	_name: _duchy.name
	_key:  _duchy.key

	_image_prefix:  "\(_name)_"
	_object_prefix: "\(_name)-"

	_images: [Name=_]: string
	_duchy_image_pull_policy: string

	_duchy_id_flags: [ for d in _duchy_names {"--duchy-ids=duchy-\(d)"}]

	_computation_control_service_flags: [
		for d in _duchy_names {
			"--computation-control-service-target=duchy-\(d)=" +
			(#Target & {name: "\(d)-computation-control-server"}).target
		},
	]

	_async_computations_control_service_target_flag: "--async-computation-control-service-target=" + (#Target & {name: "\(_name)-async-computation-control-server"}).target
	_computations_service_target_flag:               "--computations-service-target=" + (#Target & {name:              "\(_name)-spanner-computations-server"}).target
	_duchy_name_flag:                                "--duchy-name=duchy-\(_name)"
	_duchy_public_keys_config_flag:                  "--duchy-public-keys-config=" + #DuchyPublicKeysConfig
	_global_computations_service_target_flag:        "--global-computation-service-target=" + (#Target & {name: "global-computation-server"}).target
	_metric_values_service_target_flag:              "--metric-values-service-target=" + (#Target & {name:      "\(_name)-metric-values-storage-server"}).target
	_requisition_service_target_flag:                "--requisition-service-target=" + (#Target & {name:        "requisition-server"}).target
	_system_requisition_service_target_flag:         "--system-requisition-service-target=" + (#Target & {name: "system-requisition-server"}).target
	_debug_verbose_grpc_client_logging_flag:         "--debug-verbose-grpc-client-logging=\(_verbose_grpc_logging)"
	_debug_verbose_grpc_server_logging_flag:         "--debug-verbose-grpc-server-logging=\(_verbose_grpc_logging)"

	duchy_service: [Name=_]: #GrpcService & {
		_name:   _object_prefix + Name
		_system: "duchy"
	}
	duchy_service: {
		"async-computation-control-server": {}
		"computation-control-server": {}
		"spanner-computations-server": {}
		"metric-values-storage-server": {}
		"publisher-data-server": _type: "NodePort"
	}

	duchy_pod: [Name=_]: #Pod & {
		_unprefixed_name: strings.TrimSuffix(Name, "-pod")
		_name:            _object_prefix + _unprefixed_name
		_system:          "duchy"
		_image:           _images[_unprefixed_name]
		_imagePullPolicy: _duchy_image_pull_policy
	}

	duchy_pod: {
		"herald-daemon-pod": #Pod & {
			_args: [
				_computations_service_target_flag,
				_duchy_name_flag,
				_duchy_public_keys_config_flag,
				_global_computations_service_target_flag,
				"--channel-shutdown-timeout=3s",
				"--polling-interval=1m",
			]
			_dependencies: ["\(_name)-spanner-computations-server", "global-computation-server"]
		}
		"liquid-legions-v1-mill-daemon-pod": #Pod & {
			_args: [
				_computations_service_target_flag,
				_duchy_name_flag,
				_duchy_public_keys_config_flag,
				_global_computations_service_target_flag,
				_metric_values_service_target_flag,
				"--channel-shutdown-timeout=3s",
				"--duchy-secret-key=\(_key)",
				"--liquid-legions-decay-rate=23.0",
				"--liquid-legions-size=330000",
				"--mill-id=\(_name)-liquid-legions-v1-mill-1",
				"--polling-interval=1s",
			] + _computation_control_service_flags + _blob_storage_flags
			_jvm_flags: "-Xmx1g -Xms256m"
		}
		"liquid-legions-v2-mill-daemon-pod": #Pod & {
			_args: [
				_computations_service_target_flag,
				_duchy_name_flag,
				_duchy_public_keys_config_flag,
				_global_computations_service_target_flag,
				_metric_values_service_target_flag,
				"--channel-shutdown-timeout=3s",
				"--duchy-secret-key=\(_key)",
				"--liquid-legions-decay-rate=23.0",
				"--liquid-legions-size=330000",
				"--mill-id=\(_name)-liquid-legions-v2-mill-1",
				"--polling-interval=1s",
				"--noise-config=" + #LiquidLegionsV2NoiseConfig,
			] + _computation_control_service_flags + _blob_storage_flags
			_jvm_flags: "-Xmx1g -Xms256m"
			_dependencies: ["\(_name)-spanner-computations-server", "global-computation-server", "\(_name)-metric-values-storage-server", "\(_name)-computation-control-server"]
		}
		"async-computation-control-server-pod": #ServerPod & {
			_args: [
				_computations_service_target_flag,
				_duchy_name_flag,
				_duchy_public_keys_config_flag,
				_debug_verbose_grpc_server_logging_flag,
				"--port=8080",
			] + _duchy_id_flags
			_dependencies: ["\(_name)-spanner-computations-server"]
		}
		"computation-control-server-pod": #ServerPod & {
			_args: [
				_async_computations_control_service_target_flag,
				_duchy_name_flag,
				_duchy_public_keys_config_flag,
				_debug_verbose_grpc_server_logging_flag,
				"--port=8080",
			] + _duchy_id_flags + _blob_storage_flags
			_dependencies: ["\(_name)-async-computation-control-server"]
		}
		"spanner-computations-server-pod": #ServerPod & {
			_args: [
				_debug_verbose_grpc_server_logging_flag,
				_duchy_name_flag,
				_duchy_public_keys_config_flag,
				_global_computations_service_target_flag,
				"--channel-shutdown-timeout=3s",
				"--port=8080",
				"--spanner-database=\(_name)_duchy_computations",
			] + _spanner_flags
			_dependencies: ["global-computation-server"]
		}
		"metric-values-storage-server-pod": #ServerPod & {
			_args: [
				_debug_verbose_grpc_server_logging_flag,
				"--port=8080",
				"--spanner-database=\(_name)_duchy_metric_values",
			] + _spanner_flags + _blob_storage_flags
			_dependencies: ["global-computation-server"]
		}
		"publisher-data-server-pod": #ServerPod & {
			_args: [
				_debug_verbose_grpc_server_logging_flag,
				_duchy_name_flag,
				_metric_values_service_target_flag,
				_requisition_service_target_flag,
				_system_requisition_service_target_flag,
				"--duchy-public-keys-config=" + #DuchyPublicKeysConfig,
				"--port=8080",
				"--registration-service-target=127.0.0.1:9000", // TODO: change once implemented.
			]
			_dependencies: ["requisition-server", "system-requisition-server", "\(_name)-metric-values-storage-server"]
		}
	}
	setup_job: "push-spanner-schema-job": {
		apiVersion: "batch/v1"
		kind:       "Job"
		metadata: {
			name: "\(_name)-push-spanner-schema-job"
			labels: "app.kubernetes.io/name": #AppName
		}
		spec: template: spec: {
			containers: [{
				name:            "push-spanner-schema-container"
				image:           _images[name]
				imagePullPolicy: _duchy_image_pull_policy
				args:            [
							"--databases=\(_name)_duchy_computations=/app/wfa_measurement_system/src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/spanner/computations.sdl",
							"--databases=\(_name)_duchy_metric_values=/app/wfa_measurement_system/src/main/kotlin/org/wfanet/measurement/duchy/deploy/gcloud/spanner/metric_values.sdl",
				] + _spanner_schema_push_flags
			}]
			restartPolicy: "OnFailure"
		}
	}
}
