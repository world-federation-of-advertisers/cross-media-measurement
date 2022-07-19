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

#ReportingSimulator: {
	_mc_resource_name:            string
	_mc_secret_name:              string
	_simulator_image:             string
	_simulator_image_pull_policy: string | *"Always"
	_reporting_public_api_target: string

	reporting_simulator_job: #Job & {
		_name:            "reporting-simulator"
		_secretName:      _mc_secret_name
		_image:           _simulator_image
		_imagePullPolicy: _simulator_image_pull_policy
		_args:            [
          "--tls-cert-file=/var/run/secrets/files/mc_tls.pem",
          "--tls-key-file=/var/run/secrets/files/mc_tls.key",
          "--cert-collection-file=/var/run/secrets/files/all_root_certs.pem",
					"--reporting-public-api-target=\(_reporting_public_api_target)",
					"--reporting-public-api-cert-host=localhost",
					"--mc-resource-name=\(_mc_resource_name)",
		]
	}
}
