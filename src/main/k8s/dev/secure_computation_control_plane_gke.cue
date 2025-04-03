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

_controlPlaneSecretName:     string @tag("secret_name")
_publicApiAddressName:      "control-plane-public"

// Name of K8s service account for the ControlPlane internal API server.
#InternalControlPlaneServerServiceAccount: "internal-control-plan-server"

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
	controlPlane.serviceAccounts,
	controlPlane.configMaps,
	controlPlane.deployments,
	controlPlane.services,
	controlPlane.networkPolicies,
]

controlPlane: #ControlPlane & {

    _secretName:         _controlPlaneSecretName
    _verboseGrpcServerLogging: true

    _spannerConfig: database: "control-plane"

    serviceAccounts: {
		"\(#InternalControlPlaneServerServiceAccount)": #WorkloadIdentityServiceAccount & {
			_iamServiceAccountName: "control-plane-internal"
		}
	}

	configMaps: "java": #JavaConfigMap

    deployments: {
        "control-plane-internal-api-server": {
            spec: template: spec: #ServiceAccountPodSpec & {
                serviceAccountName: #InternalControlPlaneServerServiceAccount
            }
        }
        "control-plane-public-api-server": {
            _container: resources: #PublicServerResourceRequirements
        }
    }
    services: {
        "control-plane-public-api-server": _ipAddressName: _publicApiAddressName
    }
}