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

#KingdomApiTarget: #GrpcTarget & {
	target: string @tag("kingdom_public_api_target")
}

// Name of K8s service account for the internal API server.
#InternalServerServiceAccount: "internal-reporting-server"

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
	default_deny_ingress_and_egress,
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
	_kingdomApiTarget:   #KingdomApiTarget
	_internalApiTarget: certificateHost: "localhost"

	_postgresConfig: {
		iamUserLocal: "reporting-v2-internal"
		database:     "reporting-v2"
	}

	_verboseGrpcServerLogging: true

	serviceAccounts: {
		"\(#InternalServerServiceAccount)": #WorkloadIdentityServiceAccount & {
			_iamServiceAccountName: "reporting-v2-internal"
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
	}
}
