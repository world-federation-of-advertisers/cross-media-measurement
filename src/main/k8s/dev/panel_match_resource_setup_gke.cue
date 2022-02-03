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

_secret_name: string @tag("secret_name")

#GloudProject:            "halo-cmm-dev"
#ContainerRegistry:       "gcr.io"
#ContainerRegistryPrefix: #ContainerRegistry + "/" + #GloudProject
#DefaultResourceConfig: {
	replicas:              1
	resourceRequestCpu:    "100m"
	resourceLimitCpu:      "200m"
	resourceRequestMemory: "256Mi"
	resourceLimitMemory:   "512Mi"
}

objectSets: [
	panel_match_resource_setup_job,
]

panel_match_resource_setup_job: #PanelMatchResourceSetup & {
	_job_image:                  #ContainerRegistryPrefix + "/panel-match-resource-setup"
	_resource_configs:           #DefaultResourceConfig
	_resource_setup_secret_name: _secret_name
}
