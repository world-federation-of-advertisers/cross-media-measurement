// Copyright 2024 The Cross-Media Measurement Authors
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

_mc_name:                  string @tag("mc_name")
_pdp1_name:                string @tag("pdp1_name")
_pdp1_cert_name:           string @tag("pdp1_cert_name")
_population_resource_name: string @tag("population_resource_name")
_secret_name:              string @tag("secret_name")


_populationSpec:           "/etc/\(#AppName)/config-files/synthetic_population_spec_large.textproto"
_populationRequisitionFulfillerConfigs: [...#PopulationRequisitionFulfillerConfig]
_populationRequisitionFulfillerConfigs: [
    {
        dataProviderDisplayName: "pdp1"
        dataProviderResourceName: _pdp1_name
        dataProviderCertResourceName: _pdp1_cert_name
        populationKeyAndInfoList: [
            {
                populationResourceName: _population_resource_name
                populationSpecFile: _populationSpec
                eventMessageTypeUrl: "type.googleapis.com/wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
            }
        ]
        eventMessageDescriptorSet: ["/etc/\(#AppName)/config-files/person_type_set.pb", "/etc/\(#AppName)/config-files/test_event_type_set.pb"]
    }
]

objectSets: [ for fulfiller in populationRequisitionFulfillers {[fulfiller.deployment]}] +
	[ for fulfiller in populationRequisitionFulfillers {fulfiller.networkPolicies}]

populationRequisitionFulfillers: {
    for config in _populationRequisitionFulfillerConfigs {
        "\(config.dataProviderDisplayName)": #PopulationRequisitionFulfiller & {
            _imageConfig: repoSuffix: "measurement/population-requisition-fulfiller"
            _populationRequisitionFulfillerSecretName: _secret_name
            _config: config
        }
    }
}