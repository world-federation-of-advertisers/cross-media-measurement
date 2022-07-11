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
	_duchy: {
		name:                   string
		protocols_setup_config: string
		cs_cert_resource_name:  string
	}
	_duchy_secret_name: string
	_computation_control_targets: [Name=_]: string
	_kingdom_system_api_target: string
	_spannerConfig:             #SpannerConfig & {
		database: "\(_duchy.name)_duchy_computations"
	}
	_blob_storage_flags: [...string]
	_verbose_grpc_logging: "true" | "false"

	_name:                   _duchy.name
	_protocols_setup_config: _duchy.protocols_setup_config
	_cs_cert_resource_name:  _duchy.cs_cert_resource_name

	_image_prefix:  "\(_name)_"
	_object_prefix: "\(_name)-"

	_images: [Name=_]: string
	_duchy_image_pull_policy: string

	_resource_configs: [Name=_]: #ResourceConfig

	_akid_to_principal_map_file_flag:                   "--authority-key-identifier-to-principal-map-file=/etc/\(#AppName)/config-files/authority_key_identifier_to_principal_map.textproto"
	_async_computations_control_service_target_flag:    "--async-computation-control-service-target=" + (#Target & {name: "\(_name)-async-computation-control-server"}).target
	_async_computations_control_service_cert_host_flag: "--async-computation-control-service-cert-host=localhost"
	_computations_service_target_flag:                  "--computations-service-target=" + (#Target & {name: "\(_name)-spanner-computations-server"}).target
	_computations_service_cert_host_flag:               "--computations-service-cert-host=localhost"
	_duchy_name_flag:                                   "--duchy-name=\(_name)"
	_duchy_info_config_flag:                            "--duchy-info-config=/var/run/secrets/files/duchy_cert_config.textproto"
	_duchy_protocols_setup_config_flag:                 "--protocols-setup-config=/var/run/secrets/files/\(_protocols_setup_config)"
	_duchy_tls_cert_file_flag:                          "--tls-cert-file=/var/run/secrets/files/\(_name)_tls.pem"
	_duchy_tls_key_file_flag:                           "--tls-key-file=/var/run/secrets/files/\(_name)_tls.key"
	_duchy_cert_collection_file_flag:                   "--cert-collection-file=/var/run/secrets/files/all_root_certs.pem"
	_duchy_cs_cert_file_flag:                           "--consent-signaling-certificate-der-file=/var/run/secrets/files/\(_name)_cs_cert.der"
	_duchy_cs_key_file_flag:                            "--consent-signaling-private-key-der-file=/var/run/secrets/files/\(_name)_cs_private.der"
	_duchy_cs_cert_rename_name_flag:                    "--consent-signaling-certificate-resource-name=\(_cs_cert_resource_name)"
	_kingdom_system_api_target_flag:                    "--kingdom-system-api-target=\(_kingdom_system_api_target)"
	_kingdom_system_api_cert_host_flag:                 "--kingdom-system-api-cert-host=localhost"
	_debug_verbose_grpc_client_logging_flag:            "--debug-verbose-grpc-client-logging=\(_verbose_grpc_logging)"
	_debug_verbose_grpc_server_logging_flag:            "--debug-verbose-grpc-server-logging=\(_verbose_grpc_logging)"
	_computation_control_target_flags: [ for duchyId, target in _computation_control_targets {"--duchy-computation-control-target=\(duchyId)=\(target)"}]

	services: [Name=_]: #GrpcService & {
		_name:   _object_prefix + Name
		_system: "duchy"
	}
	services: {
		"async-computation-control-server": {}
		"computation-control-server": _type: "LoadBalancer"
		"spanner-computations-server": {}
		"requisition-fulfillment-server": _type: "LoadBalancer"
	}

	deployments: [Name=_]: #Deployment & {
		_unprefixed_name: strings.TrimSuffix(Name, "-deployment")
		_name:            _object_prefix + _unprefixed_name
		_secretName:      _duchy_secret_name
		_system:          "duchy"
		_image:           _images[_unprefixed_name]
		_imagePullPolicy: _duchy_image_pull_policy
		_resourceConfig:  _resource_configs[_unprefixed_name]
	}

	deployments: {
		"herald-daemon-deployment": #Deployment & {
			_args: [
				_computations_service_target_flag,
				_computations_service_cert_host_flag,
				_duchy_name_flag,
				_duchy_tls_cert_file_flag,
				_duchy_tls_key_file_flag,
				_duchy_cert_collection_file_flag,
				_duchy_protocols_setup_config_flag,
				_kingdom_system_api_target_flag,
				_kingdom_system_api_cert_host_flag,
				"--channel-shutdown-timeout=3s",
				"--polling-interval=1m",
			]
		}
		"liquid-legions-v2-mill-daemon-deployment": #Deployment & {
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
				_kingdom_system_api_target_flag,
				_kingdom_system_api_cert_host_flag,
				"--channel-shutdown-timeout=3s",
				"--polling-interval=1s",
			] + _blob_storage_flags + _computation_control_target_flags
			_jvm_flags: "-Xmx4g -Xms256m"
			_dependencies: ["\(_name)-spanner-computations-server", "\(_name)-computation-control-server"]
		}
		"async-computation-control-server-deployment": #ServerDeployment & {
			_args: [
				_computations_service_target_flag,
				_computations_service_cert_host_flag,
				_duchy_name_flag,
				_duchy_info_config_flag,
				_duchy_tls_cert_file_flag,
				_duchy_tls_key_file_flag,
				_duchy_cert_collection_file_flag,
				_debug_verbose_grpc_server_logging_flag,
				"--port=8443",
			]
		}
		"computation-control-server-deployment": #ServerDeployment & {
			_args: [
				_async_computations_control_service_target_flag,
				_async_computations_control_service_cert_host_flag,
				_duchy_name_flag,
				_duchy_info_config_flag,
				_duchy_tls_cert_file_flag,
				_duchy_tls_key_file_flag,
				_duchy_cert_collection_file_flag,
				_debug_verbose_grpc_server_logging_flag,
				"--port=8443",
			] + _blob_storage_flags
		}
		"spanner-computations-server-deployment": Deployment=#ServerDeployment & {
			_args: [
				_debug_verbose_grpc_server_logging_flag,
				_duchy_name_flag,
				_duchy_info_config_flag,
				_duchy_tls_cert_file_flag,
				_duchy_tls_key_file_flag,
				_duchy_cert_collection_file_flag,
				_kingdom_system_api_target_flag,
				_kingdom_system_api_cert_host_flag,
				"--channel-shutdown-timeout=3s",
				"--port=8443",
			] + _spannerConfig.flags

			_podSpec: _initContainers: {
				"\(_object_prefix)update-duchy-schema": {
					image:           _images["update-duchy-schema"]
					imagePullPolicy: Deployment._imagePullPolicy
					args:            _spannerConfig.flags
				}
			}
		}
		"requisition-fulfillment-server-deployment": #ServerDeployment & {
			_configMapMounts: [{
				name: "config-files"
			}]
			_args: [
				_debug_verbose_grpc_server_logging_flag,
				_akid_to_principal_map_file_flag,
				_duchy_name_flag,
				_duchy_tls_cert_file_flag,
				_duchy_tls_key_file_flag,
				_duchy_cert_collection_file_flag,
				_computations_service_target_flag,
				_computations_service_cert_host_flag,
				_kingdom_system_api_target_flag,
				_kingdom_system_api_cert_host_flag,
				"--port=8443",
			] + _blob_storage_flags
			_dependencies: ["\(_name)-spanner-computations-server"]
		}
	}

	networkPolicies: [Name=_]: #NetworkPolicy & {
		_name: _object_prefix + Name
	}
	// TODO(@wangyaopw): Consider setting GCS and spanner destinations explicityly.
	networkPolicies: {
		"spanner-computations-server": {
			_app_label: _object_prefix + "spanner-computations-server-app"
			_sourceMatchLabels: [
				_object_prefix + "herald-daemon-app",
				_object_prefix + "liquid-legions-v2-mill-daemon-app",
				_object_prefix + "async-computation-control-server-app",
				_object_prefix + "requisition-fulfillment-server-app",
			]
			_egresses: {
				// Need to send external traffic to Spanner.
				any: {}
			}
		}
		"requisition-fulfillment-server": {
			_app_label: _object_prefix + "requisition-fulfillment-server-app"
			_ingresses: {
				// External API server; allow ingress from anywhere to service port.
				gRpc: {
					ports: [{
						port: #GrpcServicePort
					}]
				}
			}
			_egresses: {
				// Need to send external traffic.
				any: {}
			}
		}
		"async-computation-controls-server": {
			_app_label: _object_prefix + "async-computation-control-server-app"
			_sourceMatchLabels: [
				_object_prefix + "computation-control-server-app",
			]
			_destinationMatchLabels: [
				_object_prefix + "spanner-computations-server-app",
			]
		}
		"computation-control-server": {
			_app_label: _object_prefix + "computation-control-server-app"
			_ingresses: {
				// External API server; allow ingress from anywhere to service port.
				gRpc: {
					ports: [{
						port: #GrpcServicePort
					}]
				}
			}
			_egresses: {
				// Need to send external traffic.
				any: {}
			}
		}
		"liquid-legions-v2-mill-daemon": {
			_app_label: _object_prefix + "liquid-legions-v2-mill-daemon-app"
			_egresses: {
				// Need to send external traffic.
				any: {}
			}
		}
		"herald-daemon": {
			_app_label: _object_prefix + "herald-daemon-app"
			_egresses: {
				// Need to send external traffic.
				any: {}
			}
		}
	}
}
