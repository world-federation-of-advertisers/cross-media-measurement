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

let KingdomPublicApiTarget = (#Target & {name: "v2alpha-public-api-server"}).target
let MountRoot = "/etc/\(#AppName)/pdp"

#PopulationRequisitionFulfillerConfig: {
	dataProviderResourceName:     string
	dataProviderCertResourceName: string
	throttlerMinimumInterval:     string | *"2s"
	eventMessageTypeUrl:          string
}

#PopulationRequisitionFulfiller: {
	_config:      #PopulationRequisitionFulfillerConfig
	_imageConfig: #ImageConfig & {
		repoSuffix: "data-provider/population-requisition-fulfiller"
	}

	deployment: #Deployment & {
		_name:   "population-requisition-fulfiller"
		_system: "population"
		_container: {
			resources: Resources={
				requests: {
					memory: _ | *"224Mi"
				}
				limits: {
					memory: _ | *Resources.requests.memory
				}
			}
			image: _imageConfig.image
			args: [
				"--kingdom-public-api-target=\(KingdomPublicApiTarget)",
				"--kingdom-public-api-cert-host=localhost",
				"--data-provider-resource-name=\(_config.dataProviderResourceName)",
				"--data-provider-certificate-resource-name=\(_config.dataProviderCertResourceName)",
				"--data-provider-encryption-private-keyset=\(MountRoot)/consent-signaling/pdp_enc_private.tink",
				"--data-provider-consent-signaling-private-key-der-file=\(MountRoot)/consent-signaling/pdp_cs_private.der",
				"--data-provider-consent-signaling-certificate-der-file=\(MountRoot)/consent-signaling/pdp_cs_cert.der",
				"--throttler-minimum-interval=\(_config.throttlerMinimumInterval)",
				"--tls-cert-file=\(MountRoot)/tls/tls.crt",
				"--tls-key-file=\(MountRoot)/tls/tls.key",
				"--cert-collection-file=\(MountRoot)/config/trusted_certs.pem",
				"--event-message-descriptor-set=\(MountRoot)/config/event_message_descriptor_set.pb",
				"--event-message-type-url=\(_config.eventMessageTypeUrl)",
			]
		}
		spec: template: spec: {
			_dependencies: [
				"v2alpha-public-api-server",
			]
			_mounts: {
				"pdp-config": #ConfigMapMount & {
					volumeMount: mountPath: "\(MountRoot)/config"
				}
				"pdp-consent-signaling": #SecretMount & {
					volumeMount: mountPath: "\(MountRoot)/consent-signaling"
				}
				"pdp-tls": #SecretMount & {
					volumeMount: mountPath: "\(MountRoot)/tls"
				}
			}
		}
	}

	networkPolicies: [Name=_]: #NetworkPolicy & {
		_name: Name
	}

	networkPolicies: {
		"\(deployment._name)": {
			_app_label: deployment.spec.template.metadata.labels.app
			_destinationMatchLabels: [
				"v2alpha-public-api-server-app",
			]
		}
	}
}
