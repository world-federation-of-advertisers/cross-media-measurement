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

_pdpName:     string @tag("pdp_name")
_pdpCertName: string @tag("pdp_cert_name")

_populationRequisitionFulfillerConfig: #PopulationRequisitionFulfillerConfig & {
	dataProviderResourceName:     _pdpName
	dataProviderCertResourceName: _pdpCertName
	eventMessageTypeUrl:          "type.googleapis.com/wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
}

objectSets: [
	[populationRequisitionFulfiller.deployment],
	populationRequisitionFulfiller.networkPolicies,
]

populationRequisitionFulfiller: #PopulationRequisitionFulfiller & {
	_config: _populationRequisitionFulfillerConfig
}
