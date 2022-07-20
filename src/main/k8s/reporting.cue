// Copyright 2022 The Cross-Media Measurement Authors
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

#Reporting: {
	_verboseGrpcServerLogging: bool | *false
	_verboseGrpcClientLogging: bool | *false

	_postgresConfig: #PostgresConfig

	_images: [Name=_]: string
	_reporting_image_pull_policy: string
	_reporting_secret_name:       string

	_resource_configs: [Name=_]: #ResourceConfig

	_reporting_tls_cert_file_flag:             "--tls-cert-file=/var/run/secrets/files/kingdom_tls.pem"
	_reporting_tls_key_file_flag:              "--tls-key-file=/var/run/secrets/files/kingdom_tls.key"
	_reporting_cert_collection_file_flag:      "--cert-collection-file=/var/run/secrets/files/all_root_certs.pem"
	_akid_to_principal_map_file_flag:        "--authority-key-identifier-to-principal-map-file=/etc/\(#AppName)/config-files/authority_key_identifier_to_principal_map.textproto"
	_measurement_consumer_to_api_key_map_file_flag: "--measurement-consumer-to-api-key-map-file=/etc/\(#AppName)/config-files/authority_key_identifier_to_principal_map.textproto"
	_debug_verbose_grpc_client_logging_flag: "--debug-verbose-grpc-client-logging=\(_verboseGrpcClientLogging)"
	_debug_verbose_grpc_server_logging_flag: "--debug-verbose-grpc-server-logging=\(_verboseGrpcServerLogging)"

	_internal_api_target_flag:    "--internal-api-target=" + (#Target & {name: "postgres-reporting-data-server"}).target
	_internal_api_cert_host_flag: "--internal-api-cert-host=localhost"

	services: [Name=_]: #GrpcService & {
		_name:   Name
		_system: "reporting"
	}
	services: {
		"postgres-reporting-data-server": {}
		"v1alpha-public-api-server": _type: "LoadBalancer"
	}

	jobs: [Name=_]: #Job & {
		_name: Name
	}

	deployments: [Name=_]: #ServerDeployment & {
		_name:                  Name
		_secretName:            _reporting_secret_name
		_system:                "reporting"
		_image:                 _images[_name]
		_imagePullPolicy:       _reporting_image_pull_policy
		_replicas:              _resource_configs[_name].replicas
		_resourceRequestMemory: _resource_configs[_name].resourceRequestMemory
		_resourceLimitMemory:   _resource_configs[_name].resourceLimitMemory
		_resourceRequestCpu:    _resource_configs[_name].resourceRequestCpu
		_resourceLimitCpu:      _resource_configs[_name].resourceLimitCpu
	}
	deployments: {
		//"postgres-reporting-data-server": Deployment={
	  "postgres-reporting-data-server": {
			_args: [
				_reporting_tls_cert_file_flag,
				_reporting_tls_key_file_flag,
				_reporting_cert_collection_file_flag,
				_debug_verbose_grpc_server_logging_flag,
				"--port=8443",
			] + _postgresConfig.flags

			//_podSpec: _initContainers: {
			//	"update-reporting-schema": InitContainer={
			//		image:           _images[InitContainer.name]
			//		imagePullPolicy: Deployment._imagePullPolicy
		  //			args:            _postgresConfig.flags
			//	}
			//}
		}

		"v1alpha-public-api-server": {
			_configMapMounts: [{
				name: "config-files"
			}]
			_args: [
				_debug_verbose_grpc_client_logging_flag,
				_debug_verbose_grpc_server_logging_flag,
				_reporting_tls_cert_file_flag,
				_reporting_tls_key_file_flag,
				_reporting_cert_collection_file_flag,
				_internal_api_target_flag,
				_internal_api_cert_host_flag,
				_akid_to_principal_map_file_flag,
				_measurement_consumer_to_api_key_map_file_flag,
				"--port=8443",
			]
			_dependencies: ["postgres-reporting-data-server"]
		}
	}

	networkPolicies: [Name=_]: #NetworkPolicy & {
		_name: Name
	}

	networkPolicies: {
		"internal-data-server": {
			_app_label: "postgres-reporting-data-server-app"
			_sourceMatchLabels: [
				"v1alpha-public-api-server-app",
			]
			_egresses: {
				any: {}
			}
		}
		"public-api-server": {
			_app_label: "v1alpha-public-api-server-app"
			_destinationMatchLabels: ["postgres-reporting-data-server-app"]
			_ingresses: {
				gRpc: {
					ports: [{
						port: #GrpcServicePort
					}]
				}
			}
		}
	}
}
