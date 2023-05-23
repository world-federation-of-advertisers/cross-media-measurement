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

_mc_name:   string @tag("mc_name")
_edp1_name: string @tag("edp1_name")
_edp2_name: string @tag("edp2_name")
_edp3_name: string @tag("edp3_name")
_edp4_name: string @tag("edp4_name")
_edp5_name: string @tag("edp5_name")
_edp6_name: string @tag("edp6_name")
_edpResourceNames: [_edp1_name, _edp2_name, _edp3_name, _edp4_name, _edp5_name, _edp6_name]
_secret_name:            string @tag("secret_name")
_cloudStorageBucket:     string @tag("cloud_storage_bucket")
_kingdomPublicApiTarget: string @tag("kingdom_public_api_target")
_duchyPublicApiTarget:   string @tag("duchy_public_api_target")

#BigQueryDataSet:               "demo"
#BigQueryTable:                 "labelled_events"
#ServiceAccount:                "simulator"
#SimulatorResourceRequirements: ResourceRequirements=#ResourceRequirements & {
	requests: {
		cpu: "300m"
		memory: "288Mi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}

objectSets: [ for edp in edp_simulators {edp}]

_cloudStorageConfig: #CloudStorageConfig & {
	bucket: _cloudStorageBucket
}
_bigQueryConfig: #BigQueryConfig & {
	dataset: #BigQueryDataSet
	table:   #BigQueryTable
}

#EdpConfig: {
	publisherId: int
}
_edpConfigs: [...#EdpConfig]
_edpConfigs: [
	for i, name in _edpResourceNames {
		publisherId:  i + 1
		resourceName: name
		displayName:  "edp\(publisherId)"
	},
]

edp_simulators: {
	for edp in _edpConfigs {
		"\(edp.displayName)": #EdpSimulator & {
			_edpConfig:                 edp
			_edp_secret_name:           _secret_name
			_duchy_public_api_target:   _duchyPublicApiTarget
			_kingdom_public_api_target: _kingdomPublicApiTarget
			_blob_storage_flags:        _cloudStorageConfig.flags
			_mc_resource_name:          _mc_name
			_additional_args:           ["--publisher-id=\(edp.publisherId)"] + _bigQueryConfig.flags
			deployment: {
				_container: {
					resources: #SimulatorResourceRequirements
				}
				spec: template: spec: #ServiceAccountPodSpec & #SpotVmPodSpec & {
					serviceAccountName: #ServiceAccount
				}
			}
		}
	}
}
