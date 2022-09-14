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

#FrontendSimulator: {
	_mc_resource_name:            string
	_mc_secret_name:              string
	_mc_api_authentication_key:   string
	_simulator_image:             string
	_simulator_image_pull_policy: string | *"Always"
	_kingdom_public_api_target:   string
	_blob_storage_flags: [...string]

	job: #Job & {
		_name:       "frontend-simulator"
		_secretName: _mc_secret_name
		_container: {
			image:           _simulator_image
			imagePullPolicy: _simulator_image_pull_policy
			args:            [
						"--tls-cert-file=/var/run/secrets/files/mc_tls.pem",
						"--tls-key-file=/var/run/secrets/files/mc_tls.key",
						"--cert-collection-file=/var/run/secrets/files/all_root_certs.pem",
						"--kingdom-public-api-target=\(_kingdom_public_api_target)",
						"--kingdom-public-api-cert-host=localhost",
						"--mc-resource-name=\(_mc_resource_name)",
						"--api-authentication-key=\(_mc_api_authentication_key)",
						"--mc-consent-signaling-cert-der-file=/var/run/secrets/files/mc_cs_cert.der",
						"--mc-consent-signaling-key-der-file=/var/run/secrets/files/mc_cs_private.der",
						"--mc-encryption-private-keyset=/var/run/secrets/files/mc_enc_private.tink",
						"--output-differential-privacy-epsilon=0.1",
						"--output-differential-privacy-delta=0.000001",
			] + _blob_storage_flags
		}
	}
}
