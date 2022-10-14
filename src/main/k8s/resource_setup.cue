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

#ResourceSetup: ResourceSetup={
	_edp_display_names: [...string]
	_duchy_ids: [...string]
	_resource_setup_secret_name: string
	_job_image:                  string
	_job_image_pull_policy:      string | *"Always"
	_dependencies: [...string]
	_edp_cert_key_files_flags:
		[
			for d in _edp_display_names {
				"--edp-consent-signaling-cert-der-files=\(d)=/var/run/secrets/files/\(d)_cs_cert.der"
			},
		] + [
			for d in _edp_display_names {
				"--edp-consent-signaling-key-der-files=\(d)=/var/run/secrets/files/\(d)_cs_private.der"
			},
		] + [
			for d in _edp_display_names {
				"--edp-encryption-public-keysets=\(d)=/var/run/secrets/files/\(d)_enc_public.tink"
			},
		]
	_mc_cert_key_files_flags: [
		"--mc-consent-signaling-cert-der-file=/var/run/secrets/files/mc_cs_cert.der",
		"--mc-consent-signaling-key-der-file=/var/run/secrets/files/mc_cs_private.der",
		"--mc-encryption-public-keyset=/var/run/secrets/files/mc_enc_public.tink",
	]
	_tls_cert_key_files_flags: [
		"--tls-cert-file=/var/run/secrets/files/kingdom_tls.pem",
		"--tls-key-file=/var/run/secrets/files/kingdom_tls.key",
		"--cert-collection-file=/var/run/secrets/files/kingdom_root.pem",
	]
	_duchy_cs_cert_files_flags: [
		for d in _duchy_ids {
			"--duchy-consent-signaling-cert-der-files=\(d)=/var/run/secrets/files/\(d)_cs_cert.der"
		},
	]
	_kingdom_public_api_flags: [
		"--kingdom-public-api-target=" + (#Target & {name: "v2alpha-public-api-server"}).target,
		"--kingdom-public-api-cert-host=localhost",
	]
	_kingdom_internal_api_flags: [
		"--kingdom-internal-api-target=" + (#Target & {name: "gcp-kingdom-data-server"}).target,
		"--kingdom-internal-api-cert-host=localhost",
	]

	resource_setup_job: #Job & {
		_name:       "resource-setup"
		_secretName: _resource_setup_secret_name
		_container: {
			image:           _job_image
			imagePullPolicy: _job_image_pull_policy
			args:
				_edp_cert_key_files_flags +
				_mc_cert_key_files_flags +
				_tls_cert_key_files_flags +
				_duchy_cs_cert_files_flags +
				_kingdom_public_api_flags +
				_kingdom_internal_api_flags
		}

		spec: {
			backoffLimit: 0 // Don't retry.
			template: spec: {
				_dependencies: ResourceSetup._dependencies
				restartPolicy: "Never"
			}
		}
	}
}
