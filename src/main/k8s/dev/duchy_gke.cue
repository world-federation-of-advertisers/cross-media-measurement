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

_duchy_name:                   string @tag("duchy_name")
_duchy_protocols_setup_config: string @tag("duchy_protocols_setup_config")
_secret_name:                  string @tag("secret_name")
_cloudStorageBucket:           string @tag("cloud_storage_bucket")
_certificateId:                string @tag("certificate_id")

_duchy_cert_name: "duchies/\(_duchy_name)/certificates/\(_certificateId)"

#KingdomSystemApiTarget:             string @tag("kingdom_system_api_target")
#InternalServerServiceAccount:       "internal-server"
#StorageServiceAccount:              "storage"
#InternalServerResourceRequirements: #ResourceRequirements & {
	requests: {
		cpu: "75m"
	}
}
#HeraldResourceRequirements: #ResourceRequirements & {
	requests: {
		cpu: "25m"
	}
}
#MillResourceRequirements: ResourceRequirements=#ResourceRequirements & {
	requests: {
		cpu:    "3"
		memory: "2Gi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}
#MillMaxHeapSize: "1G"
#MillReplicas:    1

objectSets: [
	default_deny_ingress_and_egress,
	duchy.serviceAccounts,
	duchy.configMaps,
	duchy.deployments,
	duchy.services,
	duchy.networkPolicies,
	duchy.cronjobs,
]

_cloudStorageConfig: #CloudStorageConfig & {
	bucket: _cloudStorageBucket
}

duchy: #SpannerDuchy & {
	_duchy: {
		name:                   _duchy_name
		protocols_setup_config: _duchy_protocols_setup_config
		cs_cert_resource_name:  _duchy_cert_name
	}
	_duchy_secret_name: _secret_name
	_computation_control_targets: {
		"aggregator": "system.aggregator.dev.halo-cmm.org:8443"
		"worker1":    "system.worker1.dev.halo-cmm.org:8443"
		"worker2":    "v1alpha.system.aws.worker2.dev.halo-cmm.org:8443"
	}
	_kingdom_system_api_target: #KingdomSystemApiTarget
	_blob_storage_flags:        _cloudStorageConfig.flags
	_verbose_grpc_logging:      "false"
	_duchyMillParallelism:      4

	serviceAccounts: [string]: #WorkloadIdentityServiceAccount
	serviceAccounts: {
		"\(#InternalServerServiceAccount)": {
			_iamServiceAccountName: "\(_duchy_name)-duchy-internal"
		}
		"\(#StorageServiceAccount)": {
			_iamServiceAccountName: "\(_duchy_name)-duchy-storage"
		}
	}

	configMaps: "java": #JavaConfigMap

	deployments: {
		"internal-api-server-deployment": {
			_container: {
				resources: #InternalServerResourceRequirements
			}
			spec: template: spec: #ServiceAccountPodSpec & {
				serviceAccountName: #InternalServerServiceAccount
			}
		}
		"herald-daemon-deployment": {
			_container: {
				resources: #HeraldResourceRequirements
			}
			spec: template: spec: #SpotVmPodSpec
		}
		"liquid-legions-v2-mill-daemon-deployment": {
			_container: {
				_javaOptions: maxHeapSize: #MillMaxHeapSize
				resources: #MillResourceRequirements
			}
			spec: {
				replicas: #MillReplicas
				template: spec: #ServiceAccountPodSpec & #SpotVmPodSpec & {
					serviceAccountName: #StorageServiceAccount
				}
			}
		}
		"computation-control-server-deployment": {
			spec: template: spec: #ServiceAccountPodSpec & {
				serviceAccountName: #StorageServiceAccount
			}
		}
		"requisition-fulfillment-server-deployment": {
			spec: template: spec: #ServiceAccountPodSpec & {
				serviceAccountName: #StorageServiceAccount
			}
		}
	}
}
