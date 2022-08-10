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

#EdpSimulator: EdpSimulator={
	_edp: {display_name: string, resource_name: string}
	_mc_resource_name:          string
	_edp_secret_name:           string
	_duchy_public_api_target:   string
	_kingdom_public_api_target: string
	_event_query_flag:          string
	_resourceConfig:            #ResourceConfig

	_edp_display_name:  _edp.display_name
	_edp_resource_name: _edp.resource_name

	_edp_simulator_image:         string
	_simulator_image_pull_policy: string
	_blob_storage_flags: [...string]

	_additional_args: [...string]

	edp_simulator_deployment: #Deployment & {
		_name:            _edp_display_name + "-simulator"
		_secretName:      _edp_secret_name
		_system:          "simulator"
		_image:           _edp_simulator_image
		_imagePullPolicy: _simulator_image_pull_policy
		_resourceConfig:  EdpSimulator._resourceConfig

		_args: [
			"--tls-cert-file=/var/run/secrets/files/\(_edp_display_name)_tls.pem",
			"--tls-key-file=/var/run/secrets/files/\(_edp_display_name)_tls.key",
			"--cert-collection-file=/var/run/secrets/files/all_root_certs.pem",
			"--data-provider-resource-name=\(_edp_resource_name)",
			"--data-provider-display-name=\(_edp_display_name)",
			"--data-provider-encryption-private-keyset=/var/run/secrets/files/\(_edp_display_name)_enc_private.tink",
			"--data-provider-consent-signaling-private-key-der-file=/var/run/secrets/files/\(_edp_display_name)_cs_private.der",
			"--data-provider-consent-signaling-certificate-der-file=/var/run/secrets/files/\(_edp_display_name)_cs_cert.der",
			"--mc-resource-name=\(_mc_resource_name)",
			"--kingdom-public-api-target=\(_kingdom_public_api_target)",
			"--kingdom-public-api-cert-host=localhost",
			"--requisition-fulfillment-service-target=\(_duchy_public_api_target)",
			"--requisition-fulfillment-service-cert-host=localhost",
			"--event-query=\(_event_query_flag)",
		] + _blob_storage_flags + _additional_args
	}
}
