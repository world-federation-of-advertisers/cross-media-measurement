// Copyright 2025 The Cross-Media Measurement Authors
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

#SecureComputation: {

	_verboseGrpcServerLogging: bool | *false
	_verboseGrpcClientLogging: bool | *false

	_spannerConfig: #SpannerConfig & {
		database: "secure-computation"
	}

	_secureComputationInternalApiTarget: #GrpcTarget & {
		serviceName:           "secure-computation-internal-api-server"
		certificateHost:       "localhost"
		targetOption:          "--secure-computation-internal-api-target"
		certificateHostOption: "--secure-computation-internal-api-cert-host"
	}

	_imageSuffixes: [_=string]: string
	_imageSuffixes: {
		"secure-computation-public-api-server":   string | *"secure-computation/public-api"
		"secure-computation-internal-api-server": string | *"secure-computation/internal-server"
		"update-secure-computation-schema":       string | *"secure-computation/update-schema"
	}
	_imageConfigs: [_=string]: #ImageConfig
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

	_secureComputationSecretName: string

	_debugVerboseGrpcClientLoggingFlag: "--debug-verbose-grpc-client-logging=\(_verboseGrpcClientLogging)"
	_debugVerboseGrpcServerLoggingFlag: "--debug-verbose-grpc-server-logging=\(_verboseGrpcServerLogging)"

	services: [Name=_]: #GrpcService & {
		metadata: {
			_component: "secure-computation"
			name:       Name
		}
	}

	services: {
		"secure-computation-internal-api-server": {}
		"secure-computation-public-api-server": #ExternalService
	}

	deployments: [Name=_]: #ServerDeployment & {
		_name:       Name
		_secretName: _secureComputationSecretName
		_system:     "secure-computation"
		_container: {
			image: _images[_name]
		}
	}
	deployments: {
		"secure-computation-internal-api-server": {
			_container: args: [
						_debugVerboseGrpcServerLoggingFlag,
						"--cert-collection-file=/var/run/secrets/files/secure_computation_root.pem",
						"--tls-cert-file=/var/run/secrets/files/secure_computation_tls.pem",
						"--tls-key-file=/var/run/secrets/files/secure_computation_tls.key",
						"--queue-config=/etc/\(#AppName)/config-files/queues_config.textproto",
						"--google-project-id=" + #GCloudProject,
			] + _spannerConfig.flags

			_updateSchemaContainer: Container=#Container & {
				image:            _images[Container.name]
				args:             _spannerConfig.flags
				imagePullPolicy?: _container.imagePullPolicy
			}

			spec: template: spec: {
				_mounts: {
					"config-files": #ConfigMapMount
				}
				_initContainers: {
					"update-secure-computation-schema": _updateSchemaContainer
				}
			}
		}

		"secure-computation-public-api-server": {
			_container: args: [
						_debugVerboseGrpcClientLoggingFlag,
						_debugVerboseGrpcServerLoggingFlag,
						"--cert-collection-file=/var/run/secrets/files/secure_computation_root.pem",
						"--tls-cert-file=/var/run/secrets/files/secure_computation_tls.pem",
						"--tls-key-file=/var/run/secrets/files/secure_computation_tls.key",
			] + _secureComputationInternalApiTarget.args
			spec: template: spec: {
				_dependencies: ["secure-computation-internal-api-server"]
			}
		}

	}

	networkPolicies: [Name=_]: #NetworkPolicy & {
		_name:      Name
		_app_label: _ | *"\(_name)-app"
	}

	networkPolicies: {
		"secure-computation-internal-api-server": {
			_sourceMatchLabels: ["secure-computation-public-api-server-app"]
			_egresses: {
				// Needs to call out to Spanner.
				any: {}
			}
		}
		"secure-computation-public-api-server": {
			_destinationMatchLabels: ["secure-computation-internal-api-server-app"]
			_ingresses: {
				gRpc: {
					ports: [{
						port: #GrpcPort
					}]
				}
			}
		}
	}

	configMaps: [Name=string]: #ConfigMap & {
		metadata: name: Name
	}

	serviceAccounts: [Name=string]: #ServiceAccount & {
		metadata: name: Name
	}

}
