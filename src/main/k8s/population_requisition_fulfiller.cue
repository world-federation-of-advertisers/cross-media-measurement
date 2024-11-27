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

#KingdomSystemApiTarget: (#Target & {name: "system-api-server"}).target
#KingdomPublicApiTarget: (#Target & {name: "v2alpha-public-api-server"}).target

_populationSpec: "/etc/\(#AppName)/config-files/synthetic_population_spec_large.textproto"


#PopulationRequisitionFulfillerConfig: {
    dataProviderDisplayName:     string
    dataProviderResourceName:    string
    dataProviderCertResourceName: string
    throttlerMinimumInterval:     string | *"2s"
    eventMessageDescriptorSets:   [...string]
    populationConfigs: [...#PopulationConfig]
}

#PopulationConfig: {
    populationResourceName: string
    populationSpecFile:     string
    eventMessageTypeUrl:    string
}

#PopulationRequisitionFulfiller: {
    _config: #PopulationRequisitionFulfillerConfig

    let DisplayName = _config.dataProviderDisplayName

    deployment: #Deployment & {
        _name: "population-requisition-fulfiller-" + DisplayName
        _container: {
            args: [
                "--kingdom-system-api-target=\(#KingdomSystemApiTarget)",
                "--kingdom-system-api-cert-host=localhost",
                "--data-provider-resource-name=\(_config.dataProviderResourceName)",
                "--data-provider-display-name=\(DisplayName)",
                "--data-provider-certificate-resource-name=\(_config.dataProviderCertResourceName)",
                "--data-provider-encryption-private-keyset=/var/run/secrets/files/\(DisplayName)_enc_private.tink",
                "--data-provider-consent-signaling-private-key-der-file=/var/run/secrets/files/\(DisplayName)_cs_private.der",
               	"--data-provider-consent-signaling-certificate-der-file=/var/run/secrets/files/\(DisplayName)_cs_cert.der",
                "--throttler-minimum-interval=\(_config.throttlerMinimumInterval)",
            ] + [ for set in _config.eventMessageDescriptorSets {
                "--event-message-descriptor-set=\(set)"
            }] + [ for config in _config.populationConfigs {
                "--population-resource-name=\(config.populationResourceName)"
            }] + [ for config in _config.populationConfigs {
                "--population-spec=\(_populationSpec)"
            }] + [ for config in _config.populationConfigs {
                "--event-message-type-url=\(config.eventMessageTypeUrl)"
            }]
        }
    }
}