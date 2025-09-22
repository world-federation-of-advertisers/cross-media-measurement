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
	_mcName:            string
	_privateKeyDerFile: string
	_apiKey:            string
	_edp1Name:          string
	_edp2Name:          string
	_edp3Name:          string
	_edp4Name:          string
	_edp5Name:          string
	_edp6Name:          string
	_edpResourceNames: [_edp1Name, _edp2Name, _edp3Name, _edp4Name, _edp5Name, _edp6Name]
	_verboseGrpcClientLogging: bool | *false
	_kingdomPublicApiTarget:   string
	_secretName:               string
	let SecretName = _secretName

	_privateKeyDerFileFlag:             "--private-key-der-file=/var/run/secrets/files/mc_cs_private.der"
	_tlsCertFileFlag:                   "--tls-cert-file=/var/run/secrets/files/mc_tls.pem"
	_tlsKeyFileFlag:                    "--tls-key-file=/var/run/secrets/files/mc_tls.key"
	_certCollectionFileFlag:            "--cert-collection-file=/var/run/secrets/files/all_root_certs.pem"
	_kingdomPublicApiTargetFlag:        "--kingdom-public-api-target=\(_kingdomPublicApiTarget)"
	_kingdomPublicApiCertHostFlag:      "--kingdom-public-api-cert-host=localhost"
	_debugVerboseGrpcClientLoggingFlag: "--debug-verbose-grpc-client-logging=\(_verboseGrpcClientLogging)"

	_imageSuffixes: [string]: string
	_imageSuffixes: {
		"measurement-system-prober": string | *"prober/measurement-system-prober"
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
		_secretName: SecretName
		_system:     "kingdom"
		_container: {
			image: _images[_name]
		}
	}

	cronJobs: {
		"measurement-system-prober": {
			_container: args: [
				"--measurement-consumer=\(_mcName)",
				_privateKeyDerFileFlag,
				"--api-key=\(_apiKey)",
				_tlsKeyFileFlag,
				_tlsCertFileFlag,
				_certCollectionFileFlag,
				_kingdomPublicApiTargetFlag,
				_kingdomPublicApiCertHostFlag,
				"--measurement-lookback-duration=1d",
				"--duration-between-measurements=1d",
				"--data-provider=\(_edp1Name)",
				"--data-provider=\(_edp2Name)",
				"--data-provider=\(_edp3Name)",
				"--data-provider=\(_edp4Name)",
				"--data-provider=\(_edp5Name)",
				"--data-provider=\(_edp6Name)",
			]
			spec: schedule: "* * * * *"
		}
	}
}
