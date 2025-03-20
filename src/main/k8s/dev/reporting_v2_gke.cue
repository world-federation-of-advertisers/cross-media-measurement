// Copyright 2023 The Cross-Media Measurement Authors
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
_publicApiAddressName:        string @tag("public_api_address_name")
_accessPublicApiAddressName:  "access-public"

#KingdomApiTarget: #GrpcTarget & {
	target: string @tag("kingdom_public_api_target")
}

// Name of K8s service account for the internal API server.
#InternalServerServiceAccount: "internal-reporting-server"

// Name of K8s service account for the Access internal API server.
#InternalAccessServerServiceAccount: "internal-access-server"

#InternalServerResourceRequirements: #ResourceRequirements & {
	requests: {
		cpu: "100m"
	}
}
#PublicServerResourceRequirements: ResourceRequirements=#ResourceRequirements & {
	requests: {
		cpu:    "25m"
		memory: "256Mi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}

objectSets: [
	defaultNetworkPolicies,
	reporting.serviceAccounts,
	reporting.configMaps,
	reporting.deployments,
	reporting.services,
	reporting.cronJobs,
	reporting.networkPolicies,
]

reporting: #Reporting & {
	_secretName:         _reportingSecretName
	_mcConfigSecretName: _reportingMcConfigSecretName
	_imageSuffixes: {
		"update-reporting-postgres-schema":   "reporting/v2/gcloud-postgres-update-schema"
		"postgres-internal-reporting-server": "reporting/v2/gcloud-internal-server"
	}
	_kingdomApiTarget: #KingdomApiTarget

	_postgresConfig: {
		iamUserLocal: "reporting-v2-internal"
		database:     "reporting-v2"
	}

	_verboseGrpcServerLogging: true

	serviceAccounts: {
		"\(#InternalServerServiceAccount)": #WorkloadIdentityServiceAccount & {
			_iamServiceAccountName: "reporting-v2-internal"
		}
		"\(#InternalAccessServerServiceAccount)": #WorkloadIdentityServiceAccount & {
			_iamServiceAccountName: "access-internal"
		}
	}

	configMaps: "java": #JavaConfigMap

	deployments: {
		"postgres-internal-reporting-server": {
			_container: resources: #InternalServerResourceRequirements
			spec: template: spec: #ServiceAccountPodSpec & {
				serviceAccountName: #InternalServerServiceAccount
			}
		}
		"reporting-v2alpha-public-api-server": {
			_container: resources: #PublicServerResourceRequirements
		}
		"access-internal-api-server": {
			spec: template: spec: #ServiceAccountPodSpec & {
				serviceAccountName: #InternalAccessServerServiceAccount
			}
		}
	}

	services: {
		"reporting-v2alpha-public-api-server": _ipAddressName: _publicApiAddressName
		"access-public-api-server": _ipAddressName:            _accessPublicApiAddressName
	}
}
