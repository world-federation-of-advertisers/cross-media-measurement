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

_pdpName:                string @tag("pdp_name")
_pdpCertName:            string @tag("pdp_cert_name")
_populationResourceName: string @tag("population_resource_name")
_secretName:             string @tag("secret_name")

_populationSpec:                       "/etc/\(#AppName)/config-files/population_spec_small.textproto"
_populationRequisitionFulfillerConfig: #PopulationRequisitionFulfillerConfig & {
	dataProviderDisplayName:      "pdp1"
	dataProviderResourceName:     _pdpName
	dataProviderCertResourceName: _pdpCertName
	populationKeyAndInfoList: [
		{
			populationResourceName: _populationResourceName
			populationSpecFile:     _populationSpec
			eventMessageTypeUrl:    "type.googleapis.com/wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
		},
	]
	eventMessageDescriptor: "/etc/\(#AppName)/config-files/test_event_message_descriptor_set.pb"
}

objectSets: [
	populationRequisitionFulfiller.deployment,
	populationRequisitionFulfiller.networkPolicies,
]

populationRequisitionFulfiller: #PopulationRequisitionFulfiller & {
	_imageConfig: repoSuffix: "data-provider/population-requisition-fulfiller"
	_populationRequisitionFulfillerSecretName: _secretName
	_config:                                   _populationRequisitionFulfillerConfig
}
