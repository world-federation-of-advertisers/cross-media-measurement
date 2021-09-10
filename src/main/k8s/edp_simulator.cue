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

#EdpSimulator: {
	_edp: {display_name: string, resource_name: string}
	_mc_resource_name: string

	_edp_display_name:  _edp.display_name
	_edp_resource_name: _edp.resource_name

	_edp_simulator_image:         string
	_simulator_image_pull_policy: string
	_blob_storage_flags: [...string]

	edp_simulator_pod: #Pod & {
		_name:            _edp_display_name + "-simulator"
		_system:          "simulator"
		_image:           _edp_simulator_image
		_imagePullPolicy: _simulator_image_pull_policy

		_args: [
			"--tls-cert-file=/var/run/secrets/files/\(_edp_display_name)_tls.pem",
			"--tls-key-file=/var/run/secrets/files/\(_edp_display_name)_tls.key",
			"--cert-collection-file=/var/run/secrets/files/all_root_certs.pem",
			"--data-provider-resource-name=\(_edp_resource_name)",
			"--data-provider-consent-signaling-key-der-file=/var/run/secrets/files/\(_edp_display_name)_cs_private.der",
			"--mc-resource-name=\(_mc_resource_name)",
			"--edp-sketch-reach=1000",
			"--edp-sketch-universe-size=10000000",
			"--kingdom-public-api-target=" + (#Target & {name: "v2alpha-public-api-server"}).target,
			"--kingdom-public-api-cert-host=localhost",
			"--requisition-fulfillment-service-target=" + (#Target & {name: "worker1-requisition-fulfillment-server"}).target,
			"--requisition-fulfillment-service-cert-host=localhost",
		] + _blob_storage_flags
	}
}
