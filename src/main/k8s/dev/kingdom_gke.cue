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

#GloudProject:            "halo-cmm-dev"
#SpannerInstance:         "dev-instance"
#ContainerRegistry:       "gcr.io"
#ContainerRegistryPrefix: #ContainerRegistry + "/" + #GloudProject
#DefaultResourceConfig: {
	replicas:              1
	resourceRequestCpu:    "100m"
	resourceLimitCpu:      "400m"
	resourceRequestMemory: "256Mi"
	resourceLimitMemory:   "512Mi"
}

// Name of K8s service account for the internal API server.
#InternalServerServiceAccount: "internal-server"

objectSets: [
	default_deny_ingress_and_egress,
	kingdom.deployments,
	kingdom.services,
	kingdom.networkPolicies,
]

kingdom: #Kingdom & {
	_kingdom_secret_name: _secret_name
	_spanner_flags: [
		"--spanner-database=kingdom",
		"--spanner-instance=" + #SpannerInstance,
		"--spanner-project=" + #GloudProject,
	]
	_images: {
		"gcp-kingdom-data-server":   #ContainerRegistryPrefix + "/kingdom/data-server"
		"system-api-server":         #ContainerRegistryPrefix + "/kingdom/system-api"
		"v2alpha-public-api-server": #ContainerRegistryPrefix + "/kingdom/v2alpha-public-api"
	}
	_resource_configs: {
		"gcp-kingdom-data-server":   #DefaultResourceConfig
		"system-api-server":         #DefaultResourceConfig
		"v2alpha-public-api-server": #DefaultResourceConfig
	}
	_kingdom_image_pull_policy: "Always"
	_verboseGrpcServerLogging:  true

	deployments: {
		"gcp-kingdom-data-server": {
			_podSpec: {
				serviceAccountName: #InternalServerServiceAccount
				nodeSelector: {
					"iam.gke.io/gke-metadata-server-enabled": "true"
				}
			}
		}
	}
}
