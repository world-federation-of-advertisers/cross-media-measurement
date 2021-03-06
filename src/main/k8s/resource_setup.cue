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

#ResourceSetup: {
	_edp_display_names: [...string]
	_duchy_ids: [...string]

	_edp_cert_key_files_flags:
		[
			for d in _edp_display_names {
				"--edp-consent-signaling-cert-der-files=\(d)=/var/run/secrets/files/\(d)_cs_cert.der"
			},
		] + [
			for d in _edp_display_names {
				"--edp-consent-signaling-key-der-files=\(d)=/var/run/secrets/files/\(d)_cs_key.der"
			},
		] + [
			for d in _edp_display_names {
				"--edp-encryption-public-key-der-files=\(d)=/var/run/secrets/files/\(d)_enc_public.der"
			},
		]
	_mc_cert_key_files_flags: [
		"--mc-consent-signaling-cert-der-files=/var/run/secrets/files/mc_cs_cert.der",
		"--mc-consent-signaling-key-der-file=/var/run/secrets/files/mc_cs_key.der",
		"--mc-encryption-public-key-der-file=/var/run/secrets/files/mc_enc_public.der",
	]
	_tls_cert_key_files_flags: [
		"--tls-cert-file=/var/run/secrets/files/mc_tls.pem",
		"--tls-key-file=/var/run/secrets/files/mc_tls.key",
		"--cert-collection-file=/var/run/secrets/files/all_root_certs.pem",
	]
	_duchy_cs_cert_files_flags: [
		for d in _duchy_ids {
			"--duchy-consent-signaling-cert-der-files=\(d)=/var/run/secrets/files/\(d)_cs_cert.der"
		},
	]
	_kingdom_public_api_flags: [
		"--kingdom-public-api-target=" + (#Target & {name: "gcp-kingdom-data-server"}).target,
		"--kingdom-public-api-cert-host=localhost",
	]

	_image:           string
	_imagePullPolicy: string
	_args: [...string]

	apiVersion: "batch/v1"
	kind:       "Job"
	metadata: {
		name: "resource_setup_job"
		labels: "app.kubernetes.io/name": #AppName
	}
	spec: template: spec: {
		containers: [{
			name:            "resource-setup-container"
			image:           _image
			imagePullPolicy: _imagePullPolicy
			args:            [
						_edp_cert_key_files_flags,
						_mc_cert_key_files_flags,
						_tls_cert_key_files_flags,
						_duchy_cs_cert_files_flags,
						_kingdom_public_api_flags,

			] + _args
			volumeMounts: [{
				name:      "cache-volume"
				mountPath: "/cache"
			}]
		}]
		restartPolicy: "OnFailure"
		volumes: [{
			name: "cache-volume"
			emptyDir: {}
		}]
	}
}
