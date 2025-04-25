// Copyright 2025 The Cross-Media Measurement Authors
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

_secretName:           string @tag("secret_name")
_publicApiAddressName: "secure-computation-public"

// Name of K8s service account for the ControlPlane internal API server.
#InternalSecureComputationServerServiceAccount: "internal-secure-computation-server"

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
	secureComputation.serviceAccounts,
	secureComputation.configMaps,
	secureComputation.deployments,
	secureComputation.services,
	secureComputation.networkPolicies,
]

secureComputation: #SecureComputation & {

	_secureComputationSecretName: _secretName
	_verboseGrpcServerLogging:    true

	_spannerConfig: database: "secure-computation"

	serviceAccounts: {
		"\(#InternalSecureComputationServerServiceAccount)": #WorkloadIdentityServiceAccount & {
			_iamServiceAccountName: "secure-computation-internal"
		}
	}

	configMaps: "java": #JavaConfigMap

	deployments: {
		"secure-computation-internal-api-server": {
			spec: template: spec: #ServiceAccountPodSpec & {
				serviceAccountName: #InternalSecureComputationServerServiceAccount
			}
		}
		"secure-computation-public-api-server": {
			_container: resources: #PublicServerResourceRequirements
		}
	}
	services: {
		"secure-computation-public-api-server": _ipAddressName: _publicApiAddressName
	}
}
