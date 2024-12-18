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

import "list"


#KingdomSystemApiTarget: (#Target & {name: "system-api-server"}).target

#PopulationRequisitionFulfillerConfig: {
    dataProviderDisplayName:     string
    dataProviderResourceName:    string
    dataProviderCertResourceName: string
    throttlerMinimumInterval:     string | *"2s"
    eventMessageDescriptorSet:   [...string]
    populationKeyAndInfoList: [...#PopulationKeyAndInfo]
}

#PopulationKeyAndInfo: {
    populationResourceName: string
    populationSpecFile:     string
    eventMessageTypeUrl:    string
}

#PopulationRequisitionFulfiller: {
    _config: #PopulationRequisitionFulfillerConfig
    _imageConfig: #ImageConfig
    _populationRequisitionFulfillerSecretName: string

    let displayName = _config.dataProviderDisplayName

    deployments: [Name=string]: #Deployment & {
        _name: Name,
        _secretName: _populationRequisitionFulfillerSecretName,
        _system: "population",
        _container: {
            image: _imageConfig.image
        }
    }

    deployments: {
       "population-requisition-fulfillment-server": #ServerDeployment & {
          _container: {
                args: [
                    "--kingdom-system-api-target=\(#KingdomSystemApiTarget)",
                    "--kingdom-system-api-cert-host=localhost",
                    "--data-provider-resource-name=\(_config.dataProviderResourceName)",
                    "--data-provider-display-name=\(displayName)",
                    "--data-provider-certificate-resource-name=\(_config.dataProviderCertResourceName)",
                    "--data-provider-encryption-private-keyset=/var/run/secrets/files/\(displayName)_enc_private.tink",
                    "--data-provider-consent-signaling-private-key-der-file=/var/run/secrets/files/\(displayName)_cs_private.der",
                    "--data-provider-consent-signaling-certificate-der-file=/var/run/secrets/files/\(displayName)_cs_cert.der",
                    "--throttler-minimum-interval=\(_config.throttlerMinimumInterval)",
                ]
            },
       }
    },

    networkPolicies: [Name=_]: #NetworkPolicy & {
        _name: Name
    },
    networkPolicies: {
        "population-requisition-fulfillment-server": {
            _app_label: "population-requisition-fulfillment-server-app",
            _ingresses: {
                // External API server; allow ingress from anywhere to service port.
                gRpc: {
                    ports: [{
                        port: #GrpcPort
                    }]
                }
            },
            _egresses: {
                // Need to send external traffic.
                any: {}
            }
        }
    }
}