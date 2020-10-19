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

package k8s

import ("strings")

#Duchy: {
	_duchy: {name: string, key: string}
	_duchy_names: [...string]
	_spanner_schema_push_flags: [...string]
	_spanner_flags: [...string]
	_blob_storage_flags: [...string]

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
			(#Target & {name: "\(d)-liquid-legions-computation-control-server"}).target
		},
	]

	_computation_storage_service_target_flag: "--computation-storage-service-target=" + (#Target & {name: "\(_name)-spanner-liquid-legions-computation-storage-server"}).target
	_duchy_name_flag:                         "--duchy-name=duchy-\(_name)"
	_duchy_public_keys_config_flag:           "--duchy-public-keys-config=" + #DuchyPublicKeysConfig
	_global_computations_service_target_flag: "--global-computation-service-target=" + (#Target & {name: "global-computation-server"}).target
	_metric_values_service_target_flag:       "--metric-values-service-target=" + (#Target & {name:      "\(_name)-metric-values-storage-server"}).target
	_requisition_service_target_flag:         "--requisition-service-target=" + (#Target & {name:        "requisition-server"}).target

	duchy_service: [Name=_]: #GrpcService & {
		_name:   _object_prefix + Name
		_system: "duchy"
	}
	duchy_service: {
		"liquid-legions-computation-control-server": {}
		"spanner-liquid-legions-computation-storage-server": {}
		"metric-values-storage-server": {}
		"publisher-data-server": {}
	}

	duchy_pod: [Name=_]: #Pod & {
		_unprefixed_name: strings.TrimSuffix(Name, "-pod")
		_name:            _object_prefix + _unprefixed_name
		_system:          "duchy"
		_image:           _images[_unprefixed_name]
		_imagePullPolicy: _duchy_image_pull_policy
	}

	duchy_pod: {
		"liquid-legions-herald-daemon-pod": #Pod & {
			_args: [
				_duchy_name_flag,
				_duchy_public_keys_config_flag,
				_global_computations_service_target_flag,
				"--channel-shutdown-timeout=3s",
				"--computation-storage-service-target=" + (#Target & {name: "\(_name)-spanner-liquid-legions-computation-storage-server"}).target,
				"--polling-interval=1m",
			]
		}
		"liquid-legions-mill-daemon-pod": #Pod & {
			_args: [
				_computation_storage_service_target_flag,
				_duchy_name_flag,
				_duchy_public_keys_config_flag,
				_global_computations_service_target_flag,
				_metric_values_service_target_flag,
				"--bytes-per-chunk=2000000",
				"--channel-shutdown-timeout=3s",
				"--duchy-secret-key=\(_key)",
				"--liquid-legions-decay-rate=23.0",
				"--liquid-legions-size=330000",
				"--mill-id=\(_name)-mill-1",
				"--polling-interval=1s",
			] + _computation_control_service_flags + _blob_storage_flags
		}
		"liquid-legions-computation-control-server-pod": #ServerPod & {
			_args: [
				_computation_storage_service_target_flag,
				_duchy_name_flag,
				_duchy_public_keys_config_flag,
				"--debug-verbose-grpc-server-logging=true",
				"--port=8080",
			] + _duchy_id_flags + _blob_storage_flags
		}
		"spanner-liquid-legions-computation-storage-server-pod": #ServerPod & {
			_args: [
				_duchy_name_flag,
				_duchy_public_keys_config_flag,
				_global_computations_service_target_flag,
				"--channel-shutdown-timeout=3s",
				"--debug-verbose-grpc-server-logging=true",
				"--port=8080",
				"--spanner-database=\(_name)_duchy_computations",
			] + _spanner_flags
		}
		"metric-values-storage-server-pod": #ServerPod & {
			_args: [
				"--debug-verbose-grpc-server-logging=true",
				"--port=8080",
				"--spanner-database=\(_name)_duchy_metric_values",
			] + _spanner_flags + _blob_storage_flags
		}
		"publisher-data-server-pod": #ServerPod & {
			_args: [
				_duchy_name_flag,
				_metric_values_service_target_flag,
				_requisition_service_target_flag,
				"--debug-verbose-grpc-server-logging=true",
				"--duchy-public-keys-config=" + #DuchyPublicKeysConfig,
				"--port=8080",
				"--registration-service-target=127.0.0.1:9000", // TODO: change once implemented.
			]
		}
	}
	setup_job: "push-spanner-schema-job": {
		apiVersion: "batch/v1"
		kind:       "Job"
		metadata: name: "\(_name)-push-spanner-schema-job"
		spec: template: spec: {
			containers: [{
				name:            "push-spanner-schema-container"
				image:           _images[name]
				imagePullPolicy: _duchy_image_pull_policy
				args:            [
							"--databases=\(_name)_duchy_computations=/app/wfa_measurement_system/src/main/db/gcp/computations.sdl",
							"--databases=\(_name)_duchy_metric_values=/app/wfa_measurement_system/src/main/db/gcp/metric_values.sdl",
							"--databases=\(_name)_duchy_computation_stats=/app/wfa_measurement_system/src/main/db/gcp/computation_stats.sdl",
				] + _spanner_schema_push_flags
			}]
			restartPolicy: "OnFailure"
		}
	}
}
