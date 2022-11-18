// Copyright 2021 The Cross-Media Measurement Authors
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

_secret_name: string @tag("secret_name")

// Name of K8s service account for the internal API server.
#InternalServerServiceAccount: "internal-server"

// Number of gRPC threads for the internal API server.
#InternalServerGrpcThreads: 7

// Number of gRPC threads for the system API server.
//
// This serves long-lived streaming RPCs from each Herald which will each occupy
// a thread, so this should be greater than the number of Heralds.
#SystemServerGrpcThreads: 5

#InternalServerResourceRequirements: #ResourceRequirements & {
	requests: {
		cpu:    "500m"
		memory: "352Mi"
	}
}
#PublicServerResourceRequirements: #ResourceRequirements & {
	requests: {
		memory: "256Mi"
	}
}
#SystemServerResourceRequirements: #ResourceRequirements & {
	requests: {
		memory: "256Mi"
	}
}

objectSets: [
	default_deny_ingress_and_egress,
	kingdom.deployments,
	kingdom.services,
	kingdom.networkPolicies,
]

_imageSuffixes: [string]: string
_imageSuffixes: {
	"gcp-kingdom-data-server":   "kingdom/data-server"
	"system-api-server":         "kingdom/system-api"
	"v2alpha-public-api-server": "kingdom/v2alpha-public-api"
	"update-kingdom-schema":     "kingdom/spanner-update-schema"
}
_imageConfigs: [string]: #ImageConfig
_imageConfigs: {
	for name, suffix in _imageSuffixes {
		"\(name)": {repoSuffix: suffix}
	}
}

kingdom: #Kingdom & {
	_kingdom_secret_name: _secret_name
	_spannerConfig: database: "kingdom"

	_images: {
		for name, config in _imageConfigs {
			"\(name)": config.image
		}
	}

	_kingdom_image_pull_policy: "Always"
	_verboseGrpcServerLogging:  true

	deployments: {
		"gcp-kingdom-data-server": {
			_container: {
				_grpcThreadPoolSize: #InternalServerGrpcThreads
				resources:           #InternalServerResourceRequirements
			}
			spec: template: spec: #ServiceAccountPodSpec & {
				serviceAccountName: #InternalServerServiceAccount
			}
		}
		"system-api-server": {
			_container: {
				_grpcThreadPoolSize: #SystemServerGrpcThreads
				resources:           #SystemServerResourceRequirements
			}
		}
		"v2alpha-public-api-server": {
			_container: {
				resources: #PublicServerResourceRequirements
			}
		}
	}
}
