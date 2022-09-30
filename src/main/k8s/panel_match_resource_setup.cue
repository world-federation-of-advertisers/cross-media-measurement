// Copyright 2022 The Cross-Media Measurement Authors
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

#PanelMatchResourceSetup: {
	_edp_display_name:           string
	_resource_setup_secret_name: string
	_job_image:                  string
	_job_image_pull_policy:      string | *"Always"
	_tls_cert_key_files_flags: [
		"--tls-cert-file=/var/run/secrets/files/mc_tls.pem",
		"--tls-key-file=/var/run/secrets/files/mc_tls.key",
		"--cert-collection-file=/var/run/secrets/files/all_root_certs.pem",
	]
	_kingdom_internal_api_flags: [
		"--kingdom-internal-api-target=" + (#Target & {name: "gcp-kingdom-data-server"}).target,
		"--kingdom-internal-api-cert-host=localhost",
	]
	_edp_cert_key_files_flags: [
		"--edp-display-name=\(_edp_display_name)",
		"--edp-cert-der-file=/var/run/secrets/files/edp1_cs_cert.der",
		"--edp-key-der-file=/var/run/secrets/files/edp1_cs_private.der",
		"--edp-encryption-public-keyset=/var/run/secrets/files/edp1_enc_public.tink",
	]
	_exchange_workflow_flag: [
		"--exchange-workflow=/var/run/secrets/files/exchange_workflow.textproto",
	]

	resource_setup_job: #Job & {
		_name:       "resource-setup"
		_secretName: _resource_setup_secret_name
		_container: {
			image:           _job_image
			imagePullPolicy: _job_image_pull_policy
			args:
				_tls_cert_key_files_flags +
				_kingdom_internal_api_flags +
				_edp_cert_key_files_flags +
				_exchange_workflow_flag
		}
	}
}
