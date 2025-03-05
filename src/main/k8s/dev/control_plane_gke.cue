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

_publicApiAddressName: string @tag("public_api_address_name")

#ControlPlaneApiServiceAccount: "control-plane-api-server"

#ControlPlaneResourceRequirements: #ResourceRequirements & {
    requests: {
        cpu:    "100m"
        memory: "256Mi"
    }
    limits: {
        memory: "512Mi"
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

    _verboseGrpcServerLogging: true

    _spannerConfig: database: "control-plane"

    serviceAccounts: {
		"\(#ControlPlaneApiServiceAccount)": #WorkloadIdentityServiceAccount & {
			_iamServiceAccountName: "control-plane-api"
		}
	}

    deployments: {
        "control-plane-api-server": {
            _container: resources: #ControlPlaneResourceRequirements
            spec: template: spec: #ServiceAccountPodSpec & {
                serviceAccountName: #ControlPlaneApiServiceAccount
            }
        }
    }
    services: {
        "control-plane-api-server": _ipAddressName: _publicApiAddressName
    }
}
