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

import ("strings")

#MeasurementSystemProber: {
	_mc_name:              string
	_private_key_der_file: string
	_api_key:              string
	_edp1_name:            string
	_edp2_name:            string
	_edp3_name:            string
	_edp4_name:            string
	_edp5_name:            string
	_edp6_name:            string
	_edpResourceNames: [_edp1_name, _edp2_name, _edp3_name, _edp4_name, _edp5_name, _edp6_name]
	_verboseGrpcClientLogging:  bool | *false
	_kingdom_public_api_target: string

	_private_key_der_file_flag:              "--private-key-der-file=/var/run/secrets/files/mc_cs_private.der"
	_kingdom_tls_cert_file_flag:             "--tls-cert-file=/var/run/secrets/files/kingdom_tls.pem"
	_kingdom_tls_key_file_flag:              "--tls-key-file=/var/run/secrets/files/kingdom_tls.key"
	_kingdom_cert_collection_file_flag:      "--cert-collection-file=/var/run/secrets/files/all_root_certs.pem"
	_kingdom_public_api_target_flag:         "--kingdom-public-api-target=\(_kingdom_public_api_target)"
	_kingdom_public_api_cert_host_flag:      "--kingdom-public-api-cert-host=localhost"
	_debug_verbose_grpc_client_logging_flag: "--debug-verbose-grpc-client-logging=\(_verboseGrpcClientLogging)"

	_kingdom_secret_name: string
	_imageSuffixes: [string]: string
	_imageSuffixes: {
		"measurement-system-prober": string | *"kingdom/measurement-system-prober"
	}
	_imageConfigs: [string]: #ImageConfig
	_imageConfigs: {
		for name, suffix in _imageSuffixes {
			"\(name)": {repoSuffix: suffix}
		}
	}
	_images: [string]: string
	_images: {
		for name, config in _imageConfigs {
			"\(name)": config.image
		}
	}

	cronJobs: [Name=_]: #CronJob & {
		_name:       strings.TrimSuffix(Name, "-cronjob")
		_secretName: _kingdom_secret_name
		_system:     "kingdom"
		_container: {
			image: _images[_name]
		}
	}

	cronJobs: {
		"measurement-system-prober": {
			_container: args: [
				"--measurement-consumer=\(_mc_name)",
				_private_key_der_file_flag,
				"--api-key=\(_api_key)",
				_kingdom_tls_key_file_flag,
				_kingdom_tls_cert_file_flag,
				_kingdom_cert_collection_file_flag,
				_kingdom_public_api_target_flag,
				_kingdom_public_api_cert_host_flag,
				"--measurement-lookback-duration=1d",
				"--duration-between-measurements=1d",
				"--data-provider=\(_edp1_name)",
				"--data-provider=\(_edp2_name)",
				"--data-provider=\(_edp3_name)",
				"--data-provider=\(_edp4_name)",
				"--data-provider=\(_edp5_name)",
				"--data-provider=\(_edp6_name)",
			]
			spec: schedule: "* * * * *"
		}
	}
}
