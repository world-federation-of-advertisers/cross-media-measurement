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

import "list"

#EdpConfig: {
	displayName:      string
	resourceName:     string
	certResourceName: string
	publisherId:      int
	supportHmss:      bool | *false
}

#RequisitionFulfillmentServiceConfig: {
	duchyId:              string
	duchyPublicApiTarget: string
}

#EdpSimulator: {
	_edpConfig:        #EdpConfig
	_mc_resource_name: string
	_edp_secret_name:  string
	_requisitionFulfillmentServiceConfigs: [...#RequisitionFulfillmentServiceConfig]
	_kingdom_public_api_target: string
	_logSketchDetails:          bool | *false

	let DisplayName = _edpConfig.displayName

	_imageConfig: #ImageConfig
	_additional_args: [...string]

	_requisitionFulfillmentServiceFlags: {
		let flagLists = [ for config in _requisitionFulfillmentServiceConfigs {[
			"--requisition-fulfillment-service-duchy-id=\(config.duchyId)",
			"--requisition-fulfillment-service-target=\(config.duchyPublicApiTarget)",
			"--requisition-fulfillment-service-cert-host=localhost",
		]}]
		list.FlattenN(flagLists, 2)
	}

	deployment: #Deployment & {
		let HealthFile = "/run/probe/healthy"
		_name:       DisplayName + "-simulator"
		_secretName: _edp_secret_name
		_system:     "simulator"
		_container: {
			image: _imageConfig.image
			args:  [
				"--tls-cert-file=/var/run/secrets/files/\(DisplayName)_tls.pem",
				"--tls-key-file=/var/run/secrets/files/\(DisplayName)_tls.key",
				"--cert-collection-file=/var/run/secrets/files/all_root_certs.pem",
				"--data-provider-resource-name=\(_edpConfig.resourceName)",
				"--data-provider-display-name=\(DisplayName)",
				"--data-provider-certificate-resource-name=\(_edpConfig.certResourceName)",
				"--data-provider-encryption-private-keyset=/var/run/secrets/files/\(DisplayName)_enc_private.tink",
				"--data-provider-consent-signaling-private-key-der-file=/var/run/secrets/files/\(DisplayName)_cs_private.der",
				"--data-provider-consent-signaling-certificate-der-file=/var/run/secrets/files/\(DisplayName)_cs_cert.der",
				"--mc-resource-name=\(_mc_resource_name)",
				"--kingdom-public-api-target=\(_kingdom_public_api_target)",
				"--kingdom-public-api-cert-host=localhost",
				"--log-sketch-details=\(_logSketchDetails)",
				"--health-file=\(HealthFile)",
				"--known-event-group-metadata-type=/etc/\(#AppName)/config-files/known_event_group_metadata_type_set.pb",
			] + _requisitionFulfillmentServiceFlags + _additional_args
		}
		spec: template: spec: {
			_mounts: {
				"probe": {
					volume: emptyDir: {}
				}
			}
			_containers: {
				"probe-sidecar": {
					image: "registry.k8s.io/busybox"
					args: ["/bin/sh", "-c", "while true; do sleep 30; done"]
					startupProbe: {
						exec: command: ["cat", HealthFile]
						initialDelaySeconds: 10
						periodSeconds:       1
						failureThreshold:    30
					}
					resources: Resources={
						requests: {
							cpu:    "1m"
							memory: "10Mi"
						}
						limits: memory: _ | *Resources.requests.memory
					}
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
			_egresses: {
				// Need to be able to access Kingdom.
				any: {}
			}
		}
	}
}
