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

_resourceRequirements: ResourceRequirements=#ResourceRequirements & {
	requests: {
		cpu:    "500m"
		memory: "16Gi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}
_maxHeapSize: "13G"

_populationSpec: "/etc/\(#AppName)/config-files/synthetic_population_spec_large.textproto"
_eventGroupSpecs: [
	"/etc/\(#AppName)/config-files/synthetic_event_group_spec_large_1.textproto",
	"/etc/\(#AppName)/config-files/synthetic_event_group_spec_large_2.textproto",
	"/etc/\(#AppName)/config-files/synthetic_event_group_spec_large_3.textproto",
]

objectSets: [
	serviceAccounts,
	configMaps,
	for edp in edp_simulators {[edp.deployment]},
	for edp in edp_simulators {edp.networkPolicies},
]

_edpConfigs: [...#EdpConfig]
_edpConfigs: [
	for i, name in _edpResourceNames {
		let SpecIndex = mod(i, len(_eventGroupSpecs))
		let Number = i + 1

		resourceName:     name
		certResourceName: _edpCertResourceNames[i]
		displayName:      "edp\(Number)"

		// Support HMSS on the first half of the EDPs so that we have one EDP with each event source supporting the protocol.
		if (name == _edp1_name || name == _edp2_name || name == _edp3_name) {
			supportHmss: true
		}

		eventGroupConfigs: [{
			referenceIdSuffix:     ""
			syntheticDataSpecPath: _eventGroupSpecs[SpecIndex]
			mediaTypes: ["DISPLAY", "VIDEO"]
			brandName:    "Brand \(Number)"
			campaignName: "Campaign \(Number)"
		}]
	},
]

edp_simulators: {
	for i, edp in _edpConfigs {
		"\(edp.displayName)": #EdpSimulator & {
			_edpConfig:          edp
			_populationSpecPath: _populationSpec
			_imageConfig: repoSuffix: "simulator/edp"
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
				_container: {
					_javaOptions: maxHeapSize: _maxHeapSize
					resources: _resourceRequirements
				}
				spec: template: spec: #SpotVmPodSpec & #ServiceAccountPodSpec & {
					_mounts: "config-files": #ConfigMapMount
					serviceAccountName: "\(edp.displayName)-simulator"
				}
			}
		}
	}
}

serviceAccounts: [Name=string]: #ServiceAccount & {
	metadata: name: Name
}
serviceAccounts: {
	for edp in _edpConfigs {
		let ServiceAccountName = "\(edp.displayName)-simulator"
		"\(ServiceAccountName)": #WorkloadIdentityServiceAccount & {
			_iamServiceAccountName: ServiceAccountName
		}
	}
}

configMaps: [Name=string]: #ConfigMap & {
	metadata: name: Name
}
configMaps: "java": #JavaConfigMap
