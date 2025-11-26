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
_secret_name: string @tag("secret_name")

_worker1Id: "worker1"
_worker2Id: "worker2"

#KingdomPublicApiTarget: (#Target & {name: "v2alpha-public-api-server"}).target
#Worker1PublicApiTarget: (#Target & {name: "worker1-requisition-fulfillment-server"}).target
#Worker2PublicApiTarget: (#Target & {name: "worker2-requisition-fulfillment-server"}).target

objectSets: [ for simulator in edpSimulators {[simulator.deployment]}] +
	[ for simulator in edpSimulators {simulator.networkPolicies}]

_populationSpec: "/etc/\(#AppName)/config-files/synthetic_population_spec_small.textproto"
_eventGroupSpecs: [
	"/etc/\(#AppName)/config-files/synthetic_event_group_spec_small_1.textproto",
	"/etc/\(#AppName)/config-files/synthetic_event_group_spec_small_2.textproto",
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

edpSimulators: {
	for edpConfig in _edpConfigs {
		"\(edpConfig.displayName)": #EdpSimulator & {
			_edpConfig:          edpConfig
			_populationSpecPath: _populationSpec
			_imageConfig: repoSuffix: "simulator/edp"
			_edp_secret_name:  _secret_name
			_mc_resource_name: _mc_name
			_requisitionFulfillmentServiceConfigs: [
				{
					duchyId:              _worker1Id
					duchyPublicApiTarget: #Worker1PublicApiTarget
				},
				{
					duchyId:              _worker2Id
					duchyPublicApiTarget: #Worker2PublicApiTarget
				},
			]
			_kingdom_public_api_target: #KingdomPublicApiTarget

			deployment: spec: template: spec: {
				_mounts: "config-files": #ConfigMapMount
				_dependencies: [
					"v2alpha-public-api-server",
					"worker1-requisition-fulfillment-server",
					"worker2-requisition-fulfillment-server",
				]
			}
		}
	}
}
