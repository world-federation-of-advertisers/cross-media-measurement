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

_mc_name:        string @tag("mc_name")
_edp1_name:      string @tag("edp1_name")
_edp1_cert_name: string @tag("edp1_cert_name")
_edp2_name:      string @tag("edp2_name")
_edp2_cert_name: string @tag("edp2_cert_name")
_edp3_name:      string @tag("edp3_name")
_edp3_cert_name: string @tag("edp3_cert_name")
_edp4_name:      string @tag("edp4_name")
_edp4_cert_name: string @tag("edp4_cert_name")
_edp5_name:      string @tag("edp5_name")
_edp5_cert_name: string @tag("edp5_cert_name")
_edp6_name:      string @tag("edp6_name")
_edp6_cert_name: string @tag("edp6_cert_name")
_edpResourceNames: [_edp1_name, _edp2_name, _edp3_name, _edp4_name, _edp5_name, _edp6_name]
_edpCertResourceNames: [_edp1_cert_name, _edp2_cert_name, _edp3_cert_name, _edp4_cert_name, _edp5_cert_name, _edp6_cert_name]
_secret_name:            string @tag("secret_name")
_kingdomPublicApiTarget: string @tag("kingdom_public_api_target")

_worker1Id:              string @tag("worker1_id")
_worker1PublicApiTarget: string @tag("worker1_public_api_target")
_worker2Id:              string @tag("worker2_id")
_worker2PublicApiTarget: string @tag("worker2_public_api_target")

#SimulatorServiceAccount: "simulator"

objectSets: [
	serviceAccounts,
	configMaps,
	for edp in edp_simulators {[edp.deployment]},
	for edp in edp_simulators {edp.networkPolicies},
]

_edpConfigs: [...#EdpConfig]
_edpConfigs: [
	for i, name in _edpResourceNames {
		let Number = i + 1

		resourceName:     name
		certResourceName: _edpCertResourceNames[i]
		displayName:      "edp\(Number)"
		publisherId:      Number
	},
]

edp_simulators: {
	for edp in _edpConfigs {
		"\(edp.displayName)": #EdpSimulator & {
			_edpConfig:       edp
			_edp_secret_name: _secret_name
			_requisitionFulfillmentServiceConfigs: [{
				duchyId:              _worker1Id
				duchyPublicApiTarget: _worker1PublicApiTarget
			},
				{
					duchyId:              _worker2Id
					duchyPublicApiTarget: _worker2PublicApiTarget
				},
			]
			_kingdom_public_api_target: _kingdomPublicApiTarget
			_mc_resource_name:          _mc_name

			deployment: {
				spec: template: spec: #SpotVmPodSpec & #ServiceAccountPodSpec & {
					serviceAccountName: #SimulatorServiceAccount
				}
			}
		}
	}
}

serviceAccounts: [Name=string]: #ServiceAccount & {
	metadata: name: Name
}
serviceAccounts: {
	"\(#SimulatorServiceAccount)": #WorkloadIdentityServiceAccount & {
		_iamServiceAccountName: "simulator"
	}
}

configMaps: [Name=string]: #ConfigMap & {
	metadata: name: Name
}
configMaps: "java": #JavaConfigMap
