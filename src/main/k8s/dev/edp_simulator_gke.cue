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

_mc_name:     string @tag("mc_name")
_edp1_name:   string @tag("edp1_name")
_edp2_name:   string @tag("edp2_name")
_edp3_name:   string @tag("edp3_name")
_edp4_name:   string @tag("edp4_name")
_edp5_name:   string @tag("edp5_name")
_edp6_name:   string @tag("edp6_name")
_secret_name: string @tag("secret_name")

#KingdomPublicApiTarget:  "public.kingdom.dev.halo-cmm.org:8443"
#DuchyPublicApiTarget:    "public.worker1.dev.halo-cmm.org:8443"
#GloudProject:            "halo-cmm-dev"
#CloudStorageBucket:      "halo-cmm-dev-bucket"
#ContainerRegistry:       "gcr.io"
#ContainerRegistryPrefix: #ContainerRegistry + "/" + #GloudProject
#BigQueryTableName:       "demo.labelled_events"
#ResourceConfig: {
	replicas:              1
	resourceRequestCpu:    "100m"
	resourceLimitCpu:      "400m"
	resourceRequestMemory: "256Mi"
	resourceLimitMemory:   "512Mi"
}

objectSets: [ for edp in edp_simulators {edp}]

#Edps: [
	{
		display_name:  "edp1"
		resource_name: _edp1_name
	},
	{
		display_name:  "edp2"
		resource_name: _edp2_name
	},
	{
		display_name:  "edp3"
		resource_name: _edp3_name
	},
	{
		display_name:  "edp4"
		resource_name: _edp4_name
	},
	{
		display_name:  "edp5"
		resource_name: _edp5_name
	},
	{
		display_name:  "edp6"
		resource_name: _edp6_name
	},
]

edp_simulators: {
	for edp in #Edps {
		"\(edp.display_name)": #EdpSimulator & {
			_edp:                       edp
			_edp_secret_name:           _secret_name
			_duchy_public_api_target:   #DuchyPublicApiTarget
			_kingdom_public_api_target: #KingdomPublicApiTarget
			_blob_storage_flags: [
				"--google-cloud-storage-bucket=" + #CloudStorageBucket,
				"--google-cloud-storage-project=" + #GloudProject,
			]
			_mc_resource_name:            _mc_name
			_edp_simulator_image:         #ContainerRegistryPrefix + "/loadtest/edp-simulator"
			_resource_configs:            #ResourceConfig
			_simulator_image_pull_policy: "Always"
			_additional_args: [
				"--big-query-project-name=" + #GloudProject,
				"--big-query-table-name=" + #BigQueryTableName,
			]
		}
	}
}
