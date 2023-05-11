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

#Kingdom: {
	_verboseGrpcServerLogging: bool | *false
	_verboseGrpcClientLogging: bool | *false

	_spannerConfig: #SpannerConfig

	_kingdom_secret_name: string

	_completedMeasurementsTimeToLive: string | *"180d"
	_completedMeasurementsDryRun:     bool | *false
	_pendingMeasurementsTimeToLive:   string | *"15d"
	_pendingMeasurementsDryRun:       bool | *false
	_exchangesDaysToLive:             int | *"100"
	_exchangesDryRun:                 bool | *false

	_imageSuffixes: [string]: string
	_imageSuffixes: {
		"gcp-kingdom-data-server":           string | *"kingdom/data-server"
		"system-api-server":                 string | *"kingdom/system-api"
		"v2alpha-public-api-server":         string | *"kingdom/v2alpha-public-api"
		"update-kingdom-schema":             string | *"kingdom/spanner-update-schema"
		"completed-measurements-deletion":   string | *"kingdom/completed-measurements-deletion"
		"pending-measurements-cancellation": string | *"kingdom/pending-measurements-cancellation"
		"exchanges-deletion":                string | *"kingdom/exchanges-deletion"
	}
	_imageConfigs: [string]: #ImageConfig
	_imageConfigs: {
		for name, suffix in _imageSuffixes {
			"\(name)": {repoSuffix: suffix}
		}
	}
	_images: [string]: string
	_images: {
		for name, config in _imageConfigs {
			"\(name)": config.image
		}
	}

	_duchy_info_config_flag:                 "--duchy-info-config=/var/run/secrets/files/duchy_cert_config.textproto"
	_duchy_id_config_flag:                   "--duchy-id-config=/var/run/secrets/files/duchy_id_config.textproto"
	_llv2_protocol_config_config:            "--llv2-protocol-config-config=/var/run/secrets/files/llv2_protocol_config_config.textproto"
	_kingdom_tls_cert_file_flag:             "--tls-cert-file=/var/run/secrets/files/kingdom_tls.pem"
	_kingdom_tls_key_file_flag:              "--tls-key-file=/var/run/secrets/files/kingdom_tls.key"
	_kingdom_cert_collection_file_flag:      "--cert-collection-file=/var/run/secrets/files/all_root_certs.pem"
	_kingdom_root_cert_file_flag:            "--cert-collection-file=/var/run/secrets/files/kingdom_root.pem"
	_akid_to_principal_map_file_flag:        "--authority-key-identifier-to-principal-map-file=/etc/\(#AppName)/config-files/authority_key_identifier_to_principal_map.textproto"
	_debug_verbose_grpc_client_logging_flag: "--debug-verbose-grpc-client-logging=\(_verboseGrpcClientLogging)"
	_debug_verbose_grpc_server_logging_flag: "--debug-verbose-grpc-server-logging=\(_verboseGrpcServerLogging)"

	_internal_api_target_flag:    "--internal-api-target=" + (#Target & {name: "gcp-kingdom-data-server"}).target
	_internal_api_cert_host_flag: "--internal-api-cert-host=localhost"

	_open_id_redirect_uri_flag: "--open-id-redirect-uri=https://localhost:2048"

	_kingdomCompletedMeasurementsTimeToLiveFlag:            "--time-to-live=\(_completedMeasurementsTimeToLive)"
	_kingdomCompletedMeasurementsDryRunRetentionPolicyFlag: "--dry-run=\(_completedMeasurementsDryRun)"
	_kingdomPendingMeasurementsTimeToLiveFlag:              "--time-to-live=\(_pendingMeasurementsTimeToLive)"
	_kingdomPendingMeasurementsDryRunRetentionPolicyFlag:   "--dry-run=\(_pendingMeasurementsDryRun)"
	_kingdomExchangesDaysToLiveFlag:                        "--time-to-live=\(_exchangesDaysToLive)"
	_kingdomExchangesDryRunRetentionPolicyFlag:             "--dry-run=\(_exchangesDryRun)"
	_otlpEndpoint:                                          "--otel-exporter-otlp-endpoint=\(#OpenTelemetryCollectorEndpoint)"

	services: [Name=_]: #GrpcService & {
		_name:   Name
		_system: "kingdom"
	}
	services: {
		"gcp-kingdom-data-server": {}
		"system-api-server": _type:         "LoadBalancer"
		"v2alpha-public-api-server": _type: "LoadBalancer"
	}

	jobs: [Name=_]: #Job & {
		_name: Name
	}

	deployments: [Name=string]: #ServerDeployment & {
		_name:       Name
		_secretName: _kingdom_secret_name
		_system:     "kingdom"
		_container: {
			_grpcThreadPoolSize?: int32 & >0
			_commonServerFlags: [
				"--port=8443",
				"--health-port=8080",
				if _grpcThreadPoolSize != _|_ {
					"--grpc-thread-pool-size=\(_grpcThreadPoolSize)"
				},
			]

			image: _images[_name]
		}
	}
	deployments: {
		"gcp-kingdom-data-server": {
			_container: Container={
				args: [
					_duchy_info_config_flag,
					_duchy_id_config_flag,
					_llv2_protocol_config_config,
					_kingdom_tls_cert_file_flag,
					_kingdom_tls_key_file_flag,
					// Internal Kingdom API server should only trust Kingdom certs.
					_kingdom_root_cert_file_flag,
					_debug_verbose_grpc_server_logging_flag,
				] + Container._commonServerFlags + _spannerConfig.flags
			}
			_updateSchemaContainer: Container=#Container & {
				image:            _images[Container.name]
				imagePullPolicy?: _container.imagePullPolicy
				args:             _spannerConfig.flags
			}
			spec: template: spec: {
				_initContainers: {
					"update-kingdom-schema": _updateSchemaContainer
				}
			}
		}

		"system-api-server": {
			_container: Container={
				args: [
					_debug_verbose_grpc_client_logging_flag,
					_debug_verbose_grpc_server_logging_flag,
					_duchy_info_config_flag,
					_kingdom_tls_cert_file_flag,
					_kingdom_tls_key_file_flag,
					_kingdom_cert_collection_file_flag,
					_internal_api_target_flag,
					_internal_api_cert_host_flag,
				] + Container._commonServerFlags
			}
			spec: template: spec: _dependencies: ["gcp-kingdom-data-server"]
		}

		"v2alpha-public-api-server": {
			_container: Container={
				args: [
					_debug_verbose_grpc_client_logging_flag,
					_debug_verbose_grpc_server_logging_flag,
					_llv2_protocol_config_config,
					_kingdom_tls_cert_file_flag,
					_kingdom_tls_key_file_flag,
					_kingdom_cert_collection_file_flag,
					_internal_api_target_flag,
					_internal_api_cert_host_flag,
					_akid_to_principal_map_file_flag,
					_open_id_redirect_uri_flag,
					_duchy_info_config_flag,
				] + Container._commonServerFlags
			}
			spec: template: spec: {
				_mounts: "config-files": #ConfigMapMount
				_dependencies: ["gcp-kingdom-data-server"]
			}
		}
	}

	cronjobs: [Name=_]: #CronJob & {
		_name:       strings.TrimSuffix(Name, "-cronjob")
		_secretName: _kingdom_secret_name
		_system:     "kingdom"
		_container: {
			image: _images[_name]
		}
	}

	cronjobs: {
		"completed-measurements-deletion": Cronjob={
			_container: args: [
				_internal_api_target_flag,
				_internal_api_cert_host_flag,
				_kingdom_tls_cert_file_flag,
				_kingdom_tls_key_file_flag,
				_kingdom_cert_collection_file_flag,
				_kingdomCompletedMeasurementsTimeToLiveFlag,
				_kingdomCompletedMeasurementsDryRunRetentionPolicyFlag,
				_debug_verbose_grpc_client_logging_flag,
				_otlpEndpoint,
				"--otel-service-name=\(Cronjob.metadata.name)",
			]
			spec: schedule: "15 * * * *" // Hourly, 15 minutes past the hour
		}
		"pending-measurements-cancellation": Cronjob={
			_container: args: [
				_internal_api_target_flag,
				_internal_api_cert_host_flag,
				_kingdom_tls_cert_file_flag,
				_kingdom_tls_key_file_flag,
				_kingdom_cert_collection_file_flag,
				_kingdomPendingMeasurementsTimeToLiveFlag,
				_kingdomPendingMeasurementsDryRunRetentionPolicyFlag,
				_debug_verbose_grpc_client_logging_flag,
				_otlpEndpoint,
				"--otel-service-name=\(Cronjob.metadata.name)",
			]
			spec: schedule: "45 * * * *" // Hourly, 45 minutes past the hour
		}
		"exchanges-deletion": Cronjob={
			_container: args: [
				_internal_api_target_flag,
				_internal_api_cert_host_flag,
				_kingdom_tls_cert_file_flag,
				_kingdom_tls_key_file_flag,
				_kingdom_cert_collection_file_flag,
				_kingdomExchangesDaysToLiveFlag,
				_kingdomExchangesDryRunRetentionPolicyFlag,
				_debug_verbose_grpc_client_logging_flag,
				_otlpEndpoint,
				"--otel-service-name=\(Cronjob.metadata.name)",
			]
			spec: schedule: "40 6 * * *" // Daily, 6:40 am
		}
	}

	networkPolicies: [Name=_]: #NetworkPolicy & {
		_name: Name
	}
	// TODO(@wangyaopw): Consider setting the spanner destination explicityly.
	networkPolicies: {
		"internal-data-server": {
			_app_label: "gcp-kingdom-data-server-app"
			_sourceMatchLabels: [
				"v2alpha-public-api-server-app",
				"system-api-server-app",
				"resource-setup-app",
				"completed-measurements-deletion-app",
				"pending-measurements-cancellation-app",
				"exchanges-deletion-app"
			]
			_egresses: {
				// Need to send external traffic to Spanner.
				any: {}
			}
		}
		"public-api-server": {
			_app_label: "v2alpha-public-api-server-app"
			_destinationMatchLabels: ["gcp-kingdom-data-server-app"]
			_ingresses: {
				// External API server; allow ingress from anywhere to service port.
				gRpc: {
					ports: [{
						port: #GrpcPort
					}]
				}
			}
		}
		"system-api-server": {
			_app_label: "system-api-server-app"
			_destinationMatchLabels: ["gcp-kingdom-data-server-app"]
			_ingresses: {
				// External API server; allow ingress from anywhere to service port.
				gRpc: {
					ports: [{
						port: #GrpcPort
					}]
				}
			}
		}
		"resource-setup-job": {
			_app_label: "resource-setup-app"
			_destinationMatchLabels: [
				"gcp-kingdom-data-server-app",
				"v2alpha-public-api-server-app",
				"opentelemetry-collector-app",
			]
		}
		"completed-measurements-deletion": {
			_app_label: "completed-measurements-deletion-app"
			_destinationMatchLabels: [
				"gcp-kingdom-data-server-app",
			]
		}
		"pending-measurements-cancellation": {
			_app_label: "pending-measurements-cancellation-app"
			_destinationMatchLabels: [
				"gcp-kingdom-data-server-app",
			]
		}
		"exchanges-deletion": {
			_app_label: "exchanges-deletion-app"
			_destinationMatchLabels: [
				"gcp-kingdom-data-server-app",
			]
		}
	}
}
