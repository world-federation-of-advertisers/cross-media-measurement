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
	_duchy: {name: string, protocols_setup_config: string, cs_cert_resource_name: string}
	_spanner_schema_push_flags: [...string]
	_spanner_flags: [...string]
	_blob_storage_flags: [...string]
	_verbose_grpc_logging: "true" | "false"

	_name:                   _duchy.name
	_protocols_setup_config: _duchy.protocols_setup_config
	_cs_cert_resource_name:  _duchy.cs_cert_resource_name

	_image_prefix:  "\(_name)_"
	_object_prefix: "\(_name)-"

	_images: [Name=_]: string
	_duchy_image_pull_policy: string

	_async_computations_control_service_target_flag:    "--async-computation-control-service-target=" + (#Target & {name: "\(_name)-async-computation-control-server"}).target
	_async_computations_control_service_cert_host_flag: "--async-computation-control-service-cert-host=localhost"
	_computations_service_target_flag:                  "--computations-service-target=" + (#Target & {name: "\(_name)-spanner-computations-server"}).target
	_computations_service_cert_host_flag:               "--computations-service-cert-host=localhost"
	_duchy_name_flag:                                   "--duchy-name=duchy-\(_name)"
	_duchy_info_config_flag:                            "--duchy-info-config=" + #DuchyInfoConfig
	_public_api_protocol_configs:                       "--public-api-protocol-configs=" + #PublicApiProtocolConfigs
	_duchy_protocols_setup_config_flag:                 "--protocols-setup-config=\(_protocols_setup_config)"
	_duchy_tls_cert_file_flag:                          "--tls-cert-file=/var/run/secrets/files/\(_name)_tls.pem"
	_duchy_tls_key_file_flag:                           "--tls-key-file=/var/run/secrets/files/\(_name)_tls.key"
	_duchy_cert_collection_file_flag:                   "--cert-collection-file=/var/run/secrets/files/all_root_certs.pem"
	_duchy_cs_cert_file_flag:                           "--consent-signaling-certificate-der-file=/var/run/secrets/files/\(_name)_cs_cert.der"
	_duchy_cs_key_file_flag:                            "--consent-signaling-private-key-der-file=/var/run/secrets/files/\(_name)_cs_private.der"
	_duchy_cs_cert_rename_name_flag:                    "--consent-signaling-certificate-resource-name=\(_cs_cert_resource_name)"
	_system_api_target_flag:                            "--system-api-target=" + (#Target & {name: "system-api-server"}).target
	_system_api_cert_host_flag:                         "--system-api-cert-host=localhost"
	_debug_verbose_grpc_client_logging_flag:            "--debug-verbose-grpc-client-logging=\(_verbose_grpc_logging)"
	_debug_verbose_grpc_server_logging_flag:            "--debug-verbose-grpc-server-logging=\(_verbose_grpc_logging)"

	duchy_service: [Name=_]: #GrpcService & {
		_name:   _object_prefix + Name
		_system: "duchy"
	}
	duchy_service: {
		"async-computation-control-server": {}
		"computation-control-server": {}
		"spanner-computations-server": {}
		"requisition-fulfillment-server": _type: "NodePort"
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
				_computations_service_cert_host_flag,
				_duchy_name_flag,
				_duchy_tls_cert_file_flag,
				_duchy_tls_key_file_flag,
				_duchy_cert_collection_file_flag,
				_public_api_protocol_configs,
				_duchy_protocols_setup_config_flag,
				_system_api_target_flag,
				_system_api_cert_host_flag,
				"--channel-shutdown-timeout=3s",
				"--polling-interval=1m",
			]
			_dependencies: ["\(_name)-spanner-computations-server", "system-api-server"]
		}
		"liquid-legions-v2-mill-daemon-pod": #Pod & {
			_args: [
				_computations_service_target_flag,
				_computations_service_cert_host_flag,
				_duchy_name_flag,
				_duchy_info_config_flag,
				_duchy_tls_cert_file_flag,
				_duchy_tls_key_file_flag,
				_duchy_cert_collection_file_flag,
				_duchy_cs_cert_file_flag,
				_duchy_cs_key_file_flag,
				_duchy_cs_cert_rename_name_flag,
				_system_api_target_flag,
				_system_api_cert_host_flag,
				"--channel-shutdown-timeout=3s",
				"--mill-id=\(_name)-liquid-legions-v2-mill-1",
				"--polling-interval=1s",
			] + _blob_storage_flags
			_jvm_flags: "-Xmx4g -Xms256m"
			_dependencies: ["\(_name)-spanner-computations-server", "system-api-server", "\(_name)-computation-control-server"]
		}
		"async-computation-control-server-pod": #ServerPod & {
			_args: [
				_computations_service_target_flag,
				_computations_service_cert_host_flag,
				_duchy_name_flag,
				_duchy_info_config_flag,
				_duchy_tls_cert_file_flag,
				_duchy_tls_key_file_flag,
				_duchy_cert_collection_file_flag,
				_debug_verbose_grpc_server_logging_flag,
				"--port=8080",
			]
			_dependencies: ["\(_name)-spanner-computations-server"]
		}
		"computation-control-server-pod": #ServerPod & {
			_args: [
				_async_computations_control_service_target_flag,
				_async_computations_control_service_cert_host_flag,
				_duchy_name_flag,
				_duchy_info_config_flag,
				_duchy_tls_cert_file_flag,
				_duchy_tls_key_file_flag,
				_duchy_cert_collection_file_flag,
				_debug_verbose_grpc_server_logging_flag,
				"--port=8080",
			] + _blob_storage_flags
			_dependencies: ["\(_name)-async-computation-control-server"]
		}
		"spanner-computations-server-pod": #ServerPod & {
			_args: [
				_debug_verbose_grpc_server_logging_flag,
				_duchy_name_flag,
				_duchy_info_config_flag,
				_duchy_tls_cert_file_flag,
				_duchy_tls_key_file_flag,
				_duchy_cert_collection_file_flag,
				_system_api_target_flag,
				_system_api_cert_host_flag,
				"--channel-shutdown-timeout=3s",
				"--port=8080",
				"--spanner-database=\(_name)_duchy_computations",
			] + _spanner_flags
			_dependencies: ["system-api-server"]
		}
		"requisition-fulfillment-server-pod": #ServerPod & {
			_args: [
				_debug_verbose_grpc_server_logging_flag,
				_duchy_name_flag,
				_duchy_tls_cert_file_flag,
				_duchy_tls_key_file_flag,
				_duchy_cert_collection_file_flag,
				_computations_service_target_flag,
				_computations_service_cert_host_flag,
				_system_api_target_flag,
				_system_api_cert_host_flag,
				"--port=8080",
			] + _blob_storage_flags
			_dependencies: ["system-api-server", "\(_name)-spanner-computations-server"]
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
				] + _spanner_schema_push_flags
			}]
			restartPolicy: "OnFailure"
		}
	}
}
