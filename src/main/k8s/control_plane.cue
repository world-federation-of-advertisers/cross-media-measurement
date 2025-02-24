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

    _spannerConfig:  #SpannerConfig

    _imageSuffixes: [_=string]: string
	_imageSuffixes: {
		"control-plane-api-server": string | *"control-plane/v1/v1alpha-api"
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

    services: [Name=_]: #GrpcService & {
        metadata: {
            _component: "control-plane"
            name:       Name
        }
    }

    services: {
        "control-plane-api-server": #ExternalService
    }

    deployments: [Name=_]: #ServerDeployment & {
        _name:       Name
        _system:     "control-plane"
        _container: {
            image: _images[_name]
        }
    }

    deployments: {
        "control-plane-v1alpha-api-server": {
            _container: args: [
                "--port=8443",
                "--health-port=8080",
            ] + _spannerConfig.flags
        }
    }

    networkPolicies: [Name=_]: #NetworkPolicy & {
        _name:      Name
        _app_label: _ | *"\(_name)-app"
    }

    networkPolicies: {
        "control-plane-api-server": {
            _ingresses: {
				gRpc: {
					ports: [{
						port: #GrpcPort
					}]
				}
			}
            _egresses: {
                // Needs to call out to Spanner.
                any: {}
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
