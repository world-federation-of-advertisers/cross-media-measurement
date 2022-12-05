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

_reportingSecretName:         string @tag("secret_name")
_reportingMcConfigSecretName: string @tag("mc_config_secret_name")

#KingdomApiTarget: #GrpcTarget & {
	host: "public.kingdom.dev.halo-cmm.org"
	port: 8443
}

// Name of K8s service account for the internal API server.
#InternalServerServiceAccount: "internal-reporting-server"

#InternalServerResourceRequirements: #ResourceRequirements & {
	requests: {
		memory: "256Mi"
	}
}

objectSets: [
	default_deny_ingress_and_egress,
	reporting.deployments,
	reporting.services,
	reporting.networkPolicies,
]

_imageSuffixes: [_=string]: string
_imageSuffixes: {
	"update-reporting-schema":        "reporting/postgres-update-schema"
	"postgres-reporting-data-server": "reporting/postgres-data-server"
	"v1alpha-public-api-server":      "reporting/v1alpha-public-api"
}
_imageConfigs: [_=string]: #ImageConfig
_imageConfigs: {
	for name, suffix in _imageSuffixes {
		"\(name)": {repoSuffix: suffix}
	}
}

reporting: #Reporting & {
	_secretName:         _reportingSecretName
	_mcConfigSecretName: _reportingMcConfigSecretName
	_kingdomApiTarget:   #KingdomApiTarget
	_internalApiTarget: certificateHost: "localhost"

	_postgresConfig: {
		iamUserLocal: "reporting-internal"
		database:     "reporting"
	}

	_images: {
		for name, config in _imageConfigs {
			"\(name)": config.image
		}
	}

	_imagePullPolicy:          "Always"
	_verboseGrpcServerLogging: true

	deployments: {
		"postgres-reporting-data-server": {
			_container: resources: #InternalServerResourceRequirements
			spec: template: spec: #ServiceAccountPodSpec & {
				serviceAccountName: #InternalServerServiceAccount
			}
		}
	}
}
