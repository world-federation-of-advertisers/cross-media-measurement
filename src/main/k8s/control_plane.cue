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

#ControlPlane: {

    _verboseGrpcServerLogging: bool | *false
	_verboseGrpcClientLogging: bool | *false

    _controlPlaneSpannerConfig: #SpannerConfig & {
		database: "control-plane"
	}

	_controlPlaneInternalApiTarget: #GrpcTarget & {
        serviceName:           "control-plane-internal-api-server"
        certificateHost:       "localhost"
        targetOption:          "--control-plane-internal-api-target"
        certificateHostOption: "--control-plane-internal-api-cert-host"
    }

    _imageSuffixes: [_=string]: string
    _imageSuffixes: {
        "control_plane_v1alpha_public_api_server_image":    string | *"secure-computation/control-plane-api-v1alpha"
        "gcloud_control_plane_internal_api_server_image":   string | *"secure-computation/control-plane-internal-server"
        "gcloud_control_plane_update_schema_image":         string | *"control-plane/update-schema"
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

    _secretName:         string

    _debugVerboseGrpcClientLoggingFlag: "--debug-verbose-grpc-client-logging=\(_verboseGrpcClientLogging)"
    _debugVerboseGrpcServerLoggingFlag: "--debug-verbose-grpc-server-logging=\(_verboseGrpcServerLogging)"

    services: [Name=_]: #GrpcService & {
        metadata: {
            _component: "control-plane"
            name:       Name
        }
    }

    services: {
        "control-plane-internal-api-server": {}
        "control-plane-public-api-server": #ExternalService
    }

    deployments: [Name=_]: #ServerDeployment & {
        _name:       Name
        _secretName: ControlPlane._secretName
        _system:     "control-plane"
        _container: {
            image: _images[_name]
        }
    }
    deployments: {
        "control-plane-internal-api-server": {
            _container: args: [
                        _debugVerboseGrpcServerLoggingFlag,
                        "--cert-collection-file=/var/run/secrets/files/control_plane_root.pem",
                        "--tls-cert-file=/var/run/secrets/files/control_plane_tls.pem",
                        "--tls-key-file=/var/run/secrets/files/control_plane_tls.key",
                        "--queue-config=/etc/\(#AppName)/securecomputation-config/queue_config.textproto",
            ] + _controlPlaneSpannerConfig.flags

            _updateSchemaContainer: Container=#Container & {
                image:            _images[Container.name]
                args:             _controlPlaneSpannerConfig.flags
                imagePullPolicy?: _container.imagePullPolicy
            }

            spec: template: spec: {
                _mounts: {
                    "config-files":  #ConfigMapMount
                    "control-plane-config": #ConfigMapMount
                }
                _initContainers: {
                    "update-control-plane-schema": _updateSchemaContainer
                }
            }
        }

        "control-plane-public-api-server": {
            _container: args: [
                        _debugVerboseGrpcClientLoggingFlag,
                        _debugVerboseGrpcServerLoggingFlag,
                        "--cert-collection-file=/var/run/secrets/files/control_plane_root.pem",
                        "--tls-cert-file=/var/run/secrets/files/control_plane_tls.pem",
                        "--tls-key-file=/var/run/secrets/files/control_plane_tls.key",
            ] + _controlPlaneInternalApiTarget.args
            spec: template: spec: {
                _dependencies: ["control-plane-internal-api-server"]
            }
        }

    }

    networkPolicies: [Name=_]: #NetworkPolicy & {
        _name:      Name
        _app_label: _ | *"\(_name)-app"
    }

    networkPolicies: {
        "control-plane-internal-api-server": {
            _sourceMatchLabels: ["control-plane-public-api-server-app"]
            _egresses: {
                // Needs to call out to Spanner.
                any: {}
            }
        }
        "control-plane-public-api-server": {
            _destinationMatchLabels: ["control-plane-internal-api-server-app"]
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
    configMaps: "control-plane-config": {
        data: {
            "queue_config.textproto": #PermissionsConfig
        }
    }

	serviceAccounts: [Name=string]: #ServiceAccount & {
		metadata: name: Name
	}

}