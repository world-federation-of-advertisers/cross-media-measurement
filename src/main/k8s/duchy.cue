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

#TerminalComputationState: "SUCCEEDED" | "FAILED" | "CANCELLED"

#Duchy: {
	_duchy: {
		name:                   string
		protocols_setup_config: string
		cs_cert_resource_name:  string
	}
	_duchy_secret_name: string
	_computation_control_targets: [Name=_]: string
	_deletableComputationStates: [...#TerminalComputationState] | *[]
	_computationsTimeToLive:     string | *"180d"
	_duchyMillParallelism:       uint | *2
	_kingdom_system_api_target:  string
	_blob_storage_flags: [...string]
	_verbose_grpc_logging: "true" | "false"

	_name:                   _duchy.name
	_protocols_setup_config: _duchy.protocols_setup_config
	_cs_cert_resource_name:  _duchy.cs_cert_resource_name

	_object_prefix: "\(_name)-"

	_imageSuffixes: [string]: string
	_imageSuffixes: {
		"async-computation-control-server": string | *"duchy/async-computation-control"
		"computation-control-server":       string | *"duchy/computation-control"
		"herald-daemon":                    string | *"duchy/herald"
		"liquid-legions-v2-mill-daemon":    string | *"duchy/liquid-legions-v2-mill"
		"requisition-fulfillment-server":   string | *"duchy/requisition-fulfillment"
		"computations-cleaner":             string | *"duchy/computations-cleaner"
	}
	_imageConfigs: [string]: #ImageConfig
	_imageConfigs: {
		for name, suffix in _imageSuffixes {
			"\(name)": {repoSuffix: suffix}
		}
	}
	_images: {
		for name, config in _imageConfigs {
			"\(name)": config.image
		}
	}

	_millPollingInterval?: string
	_duchyInternalServerContainerArgs: [...string]

	_akid_to_principal_map_file_flag:                   "--authority-key-identifier-to-principal-map-file=/etc/\(#AppName)/config-files/authority_key_identifier_to_principal_map.textproto"
	_async_computations_control_service_target_flag:    "--async-computation-control-service-target=" + (#Target & {name: "\(_name)-async-computation-control-server"}).target
	_async_computations_control_service_cert_host_flag: "--async-computation-control-service-cert-host=localhost"
	_duchy_name_flag:                                   "--duchy-name=\(_name)"
	_duchy_info_config_flag:                            "--duchy-info-config=/var/run/secrets/files/duchy_cert_config.textproto"
	_duchy_protocols_setup_config_flag:                 "--protocols-setup-config=/var/run/secrets/files/\(_protocols_setup_config)"
	_duchy_tls_cert_file_flag:                          "--tls-cert-file=/var/run/secrets/files/\(_name)_tls.pem"
	_duchy_tls_key_file_flag:                           "--tls-key-file=/var/run/secrets/files/\(_name)_tls.key"
	_duchy_cert_collection_file_flag:                   "--cert-collection-file=/var/run/secrets/files/all_root_certs.pem"
	_duchyInternalApiTargetFlag:                        "--computations-service-target=" + (#Target & {name: "\(_name)-internal-api-server"}).target
	_duchyInternalApiCertHostFlag:                      "--computations-service-cert-host=localhost"
	_duchyComputationsTimeToLiveFlag:                   "--computations-time-to-live=\(_computationsTimeToLive)"
	_duchyDryRunRetentionPolicyFlag:                    "--dry-run"
	_duchyMillParallelismFlag:                          "--parallelism=\(_duchyMillParallelism)"
	_duchy_cs_cert_file_flag:                           "--consent-signaling-certificate-der-file=/var/run/secrets/files/\(_name)_cs_cert.der"
	_duchy_cs_key_file_flag:                            "--consent-signaling-private-key-der-file=/var/run/secrets/files/\(_name)_cs_private.der"
	_duchy_cs_cert_rename_name_flag:                    "--consent-signaling-certificate-resource-name=\(_cs_cert_resource_name)"
	_duchyDeletableStatesFlag: [ for state in _deletableComputationStates {"--deletable-computation-state=\(state)"}]
	_kingdom_system_api_target_flag:         "--kingdom-system-api-target=\(_kingdom_system_api_target)"
	_kingdom_system_api_cert_host_flag:      "--kingdom-system-api-cert-host=localhost"
	_debug_verbose_grpc_client_logging_flag: "--debug-verbose-grpc-client-logging=\(_verbose_grpc_logging)"
	_debug_verbose_grpc_server_logging_flag: "--debug-verbose-grpc-server-logging=\(_verbose_grpc_logging)"
	_computation_control_target_flags: [ for duchyId, target in _computation_control_targets {"--duchy-computation-control-target=\(duchyId)=\(target)"}]
	_otlpEndpoint: "--otel-exporter-otlp-endpoint=\(#OpenTelemetryCollectorEndpoint)"

	services: [Name=_]: #GrpcService & {
		_name:   _object_prefix + Name
		_system: "duchy"
	}
	services: {
		"async-computation-control-server": {}
		"computation-control-server": _type: "LoadBalancer"
		"internal-api-server": {}
		"requisition-fulfillment-server": _type: "LoadBalancer"
	}

	deployments: [Name=_]: #Deployment & {
		_unprefixed_name: strings.TrimSuffix(Name, "-deployment")
		_name:            _object_prefix + _unprefixed_name
		_secretName:      _duchy_secret_name
		_system:          "duchy"
		_container: {
			image: _images[_unprefixed_name]
		}
	}

	deployments: {
		"herald-daemon-deployment": {
			_container: args: [
						_duchyInternalApiTargetFlag,
						_duchyInternalApiCertHostFlag,
						_duchy_name_flag,
						_duchy_tls_cert_file_flag,
						_duchy_tls_key_file_flag,
						_duchy_cert_collection_file_flag,
						_duchy_protocols_setup_config_flag,
						_kingdom_system_api_target_flag,
						_kingdom_system_api_cert_host_flag,
						_debug_verbose_grpc_client_logging_flag,
			] + _duchyDeletableStatesFlag
			spec: template: spec: _dependencies: [
				"\(_name)-internal-api-server",
			]
		}
		"liquid-legions-v2-mill-daemon-deployment": Deployment={
			_workLockDuration?: string
			_container: args: [
						_duchyInternalApiTargetFlag,
						_duchyInternalApiCertHostFlag,
						_duchy_name_flag,
						_duchy_info_config_flag,
						_duchy_tls_cert_file_flag,
						_duchy_tls_key_file_flag,
						_duchyMillParallelismFlag,
						_duchy_cert_collection_file_flag,
						_duchy_cs_cert_file_flag,
						_duchy_cs_key_file_flag,
						_duchy_cs_cert_rename_name_flag,
						_kingdom_system_api_target_flag,
						_kingdom_system_api_cert_host_flag,
						if (_millPollingInterval != _|_) {"--polling-interval=\(_millPollingInterval)"},
						if (_workLockDuration != _|_) {"--work-lock-duration=\(_workLockDuration)"},
						_otlpEndpoint,
						"--otel-service-name=\(Deployment.metadata.name)",
			] + _blob_storage_flags + _computation_control_target_flags
			spec: template: spec: _dependencies: [
				"\(_name)-internal-api-server", "\(_name)-computation-control-server",
			]
		}
		"async-computation-control-server-deployment": #ServerDeployment & {
			_container: args: [
				_duchyInternalApiTargetFlag,
				_duchyInternalApiCertHostFlag,
				_duchy_name_flag,
				_duchy_info_config_flag,
				_duchy_tls_cert_file_flag,
				_duchy_tls_key_file_flag,
				_duchy_cert_collection_file_flag,
				_debug_verbose_grpc_server_logging_flag,
				"--port=8443",
				"--health-port=8080",
			]
		}
		"computation-control-server-deployment": #ServerDeployment & {
			_container: args: [
						_async_computations_control_service_target_flag,
						_async_computations_control_service_cert_host_flag,
						_duchy_name_flag,
						_duchy_info_config_flag,
						_duchy_tls_cert_file_flag,
						_duchy_tls_key_file_flag,
						_duchy_cert_collection_file_flag,
						_debug_verbose_grpc_server_logging_flag,
						"--port=8443",
						"--health-port=8080",
			] + _blob_storage_flags
		}
		"internal-api-server-deployment": #ServerDeployment & {
			_container: args: [
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
						"--health-port=8080",
			] + _duchyInternalServerContainerArgs + _blob_storage_flags
			_updateSchemaContainer: #Container & {
				image:            _images["update-duchy-schema"]
				imagePullPolicy?: _container.imagePullPolicy
				args:             _duchyInternalServerContainerArgs
			}
			spec: template: spec: {
				_initContainers: {
					"\(_object_prefix)update-duchy-schema": _updateSchemaContainer
				}
			}
		}
		"requisition-fulfillment-server-deployment": #ServerDeployment & {
			_container: args: [
						_debug_verbose_grpc_server_logging_flag,
						_akid_to_principal_map_file_flag,
						_duchy_name_flag,
						_duchy_tls_cert_file_flag,
						_duchy_tls_key_file_flag,
						_duchy_cert_collection_file_flag,
						_duchyInternalApiTargetFlag,
						_duchyInternalApiCertHostFlag,
						_kingdom_system_api_target_flag,
						_kingdom_system_api_cert_host_flag,
						"--port=8443",
						"--health-port=8080",
			] + _blob_storage_flags
			spec: template: spec: {
				_mounts: "config-files": #ConfigMapMount
				_dependencies: ["\(_name)-internal-api-server"]
			}
		}
	}

	cronjobs: [Name=_]: #CronJob & {
		_unprefixed_name: strings.TrimSuffix(Name, "-cronjob")
		_name:            _object_prefix + _unprefixed_name
		_secretName:      _duchy_secret_name
		_system:          "duchy"
		_container: {
			image: _images[_unprefixed_name]
		}
	}

	cronjobs: {
		"computations-cleaner": {
			_container: args: [
				_duchyInternalApiTargetFlag,
				_duchyInternalApiCertHostFlag,
				_duchy_tls_cert_file_flag,
				_duchy_tls_key_file_flag,
				_duchy_cert_collection_file_flag,
				_duchyComputationsTimeToLiveFlag,
				_duchyDryRunRetentionPolicyFlag,
				_debug_verbose_grpc_client_logging_flag,
			]
			spec: schedule: "0 * * * *" // Every hour
		}
	}

	networkPolicies: [Name=_]: #NetworkPolicy & {
		_name: _object_prefix + Name
	}
	// TODO(@wangyaopw): Consider setting GCS and spanner destinations explicityly.
	networkPolicies: {
		"internal-api-server": {
			_app_label: _object_prefix + "internal-api-server-app"
			_sourceMatchLabels: [
				_object_prefix + "herald-daemon-app",
				_object_prefix + "liquid-legions-v2-mill-daemon-app",
				_object_prefix + "async-computation-control-server-app",
				_object_prefix + "requisition-fulfillment-server-app",
				_object_prefix + "computations-cleaner-app",
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
						port: #GrpcPort
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
				_object_prefix + "internal-api-server-app",
				"opentelemetry-collector-app",
			]
		}
		"computation-control-server": {
			_app_label: _object_prefix + "computation-control-server-app"
			_ingresses: {
				// External API server; allow ingress from anywhere to service port.
				gRpc: {
					ports: [{
						port: #GrpcPort
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
		"computations-cleaner": {
			_app_label: _object_prefix + "computations-cleaner-app"
			_destinationMatchLabels: [
				_object_prefix + "internal-api-server-app",
			]
		}
	}

	configMaps: [Name=string]: #ConfigMap & {
		metadata: name: Name
	}

	serviceAccounts: [Name=string]: #ServiceAccount & {
		metadata: name: Name
	}
}
