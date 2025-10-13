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

let MountRoot = "/etc/\(#AppName)/edp-aggregator"

#EdpAggregator: {

	_verboseGrpcServerLogging: bool | *false
	_verboseGrpcClientLogging: bool | *false

	_spannerConfig: #SpannerConfig

	_edpAggregatorInternalApiTarget: #GrpcTarget & {
		serviceName:           "edp-aggregator-internal-api-server"
		certificateHost:       "localhost"
		targetOption:          "--edp-aggregator-internal-api-target"
		certificateHostOption: "--edp-aggregator-internal-api-cert-host"
	}

	_imageSuffixes: [_=string]: string
	_imageSuffixes: {
		"edp-aggregator-system-api-server":   string | *"edp-aggregator/system-api"
		"edp-aggregator-internal-api-server": string | *"edp-aggregator/internal-api"
		"update-edp-aggregator-schema":       string | *"edp-aggregator/update-schema"
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

	_edpAggregatorSecretName: string

	_debugVerboseGrpcClientLoggingFlag: "--debug-verbose-grpc-client-logging=\(_verboseGrpcClientLogging)"
	_debugVerboseGrpcServerLoggingFlag: "--debug-verbose-grpc-server-logging=\(_verboseGrpcServerLogging)"

	services: [Name=_]: #GrpcService & {
		metadata: {
			_component: "edp-aggregator"
			name:       Name
		}
	}

	services: {
		"edp-aggregator-internal-api-server": {}
		"edp-aggregator-system-api-server": #ExternalService
	}

	deployments: [Name=_]: #ServerDeployment & {
		_name:       Name
		_secretName: _edpAggregatorSecretName
		_system:     "edp-aggregator"
		_container: {
			image: _images[_name]
		}
	}
	deployments: {
		"edp-aggregator-internal-api-server": {
			_container: args: [
						_debugVerboseGrpcServerLoggingFlag,
						"--cert-collection-file=\(MountRoot)/config/trusted_certs.pem",
						"--tls-cert-file=\(MountRoot)/tls/tls.crt",
						"--tls-key-file=\(MountRoot)/tls/tls.key",
			] + _spannerConfig.flags

			_updateSchemaContainer: Container=#Container & {
				image:            _images[Container.name]
				args:             _spannerConfig.flags
				imagePullPolicy?: _container.imagePullPolicy
			}

			spec: template: spec: {
				_initContainers: {
					"update-edp-aggregator-schema": _updateSchemaContainer
				}
				_mounts: {
					"edp-aggregator-tls": #SecretMount & {
						volumeMount: mountPath: "\(MountRoot)/tls"
					}
					"edp-aggregator-config": #ConfigMapMount & {
						volumeMount: mountPath: "\(MountRoot)/config"
					}
				}
			}
		}

		"edp-aggregator-system-api-server": {
			_container: args: [
						_debugVerboseGrpcClientLoggingFlag,
						_debugVerboseGrpcServerLoggingFlag,
						"--cert-collection-file=\(MountRoot)/config/trusted_certs.pem",
						"--tls-cert-file=\(MountRoot)/tls/tls.crt",
						"--tls-key-file=\(MountRoot)/tls/tls.key",
			] + _edpAggregatorInternalApiTarget.args
			spec: template: spec: {
				_dependencies: ["edp-aggregator-internal-api-server"]
				_mounts: {
					"edp-aggregator-tls": #SecretMount & {
						volumeMount: mountPath: "\(MountRoot)/tls"
					}
					"edp-aggregator-config": #ConfigMapMount & {
						volumeMount: mountPath: "\(MountRoot)/config"
					}
				}
			}
		}

	}

	networkPolicies: [Name=_]: #NetworkPolicy & {
		_name:      Name
		_app_label: _ | *"\(_name)-app"
	}

	networkPolicies: {
		"edp-aggregator-internal-api-server": {
			_sourceMatchLabels: ["edp-aggregator-system-api-server-app"]
			_egresses: {
				// Needs to call out to Spanner.
				any: {}
			}
		}
		"edp-aggregator-system-api-server": {
			_destinationMatchLabels: ["edp-aggregator-internal-api-server-app"]
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
