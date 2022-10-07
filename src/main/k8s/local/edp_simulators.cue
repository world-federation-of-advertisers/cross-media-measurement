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
_secret_name: string @tag("secret_name")

#KingdomPublicApiTarget: (#Target & {name: "v2alpha-public-api-server"}).target
#Worker1PublicApiTarget: (#Target & {name: "worker1-requisition-fulfillment-server"}).target

objectSets: [ for simulator in edpSimulators {simulator}]

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

edpSimulators: {
	for edpConfig in _edpConfigs {
		"\(edpConfig.displayName)": #EdpSimulator & {
			_edpConfig:                 edpConfig
			_edp_secret_name:           _secret_name
			_mc_resource_name:          _mc_name
			_duchy_public_api_target:   #Worker1PublicApiTarget
			_kingdom_public_api_target: #KingdomPublicApiTarget
			_blob_storage_flags: [
				"--forwarded-storage-service-target=" + (#Target & {name: "fake-storage-server"}).target,
				"--forwarded-storage-cert-host=localhost",
			]
			_edp_simulator_image:         "bazel/src/main/kotlin/org/wfanet/measurement/loadtest/dataprovider:forwarded_storage_edp_simulator_runner_image"
			_simulator_image_pull_policy: "Never"

			deployment: spec: template: spec: {
				_dependencies: [
					"v2alpha-public-api-server",
					"worker1-requisition-fulfillment-server",
				]
			}
		}
	}
}
