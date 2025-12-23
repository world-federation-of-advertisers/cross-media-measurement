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

_duchy_name:                     string @tag("duchy_name")
_duchy_protocols_setup_config:   string @tag("duchy_protocols_setup_config")
_secret_name:                    string @tag("secret_name")
_cloudStorageBucket:             string @tag("cloud_storage_bucket")
_certificateId:                  string @tag("certificate_id")
_publicApiAddressName:           string @tag("public_api_address_name")
_systemApiAddressName:           string @tag("system_api_address_name")
_internalApiAddressName:         string @tag("internal_api_address_name")
_trusteeMillSubnetworkCidrRange: string @tag("trustee_mill_subnetwork_cidr_range")
_aggregatorSystemApiTarget:      string @tag("aggregator_system_api_target")
_worker1SystemApiTarget:         string @tag("worker1_system_api_target")
_worker2SystemApiTarget:         string @tag("worker2_system_api_target")
_duchyKeyEncryptionKeyFile:      string @tag("duchy_key_encryption_key_file")

_duchy_cert_name: "duchies/\(_duchy_name)/certificates/\(_certificateId)"

#KingdomSystemApiTarget:             string @tag("kingdom_system_api_target")
#KingdomPublicApiTarget:             string @tag("kingdom_public_api_target")
#InternalServerServiceAccount:       "internal-server"
#StorageServiceAccount:              "storage"
#InternalServerResourceRequirements: #ResourceRequirements & {
	requests: {
		cpu: "75m"
	}
}
#HeraldResourceRequirements: ResourceRequirements=#ResourceRequirements & {
	requests: {
		cpu:    "25m"
		memory: "512Mi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}
#HeraldMaxHeapSize:            "400M"
#Llv2MillResourceRequirements: ResourceRequirements=#ResourceRequirements & {
	requests: {
		cpu:    "3"
		memory: "2.5Gi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}
#Llv2MillMaxHeapSize:          "1G"
#Llv2MillMaxConcurrency:       10
#HmssMillResourceRequirements: ResourceRequirements=#ResourceRequirements & {
	requests: {
		cpu:    "2"
		memory: "6Gi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}
#HmssMillMaxHeapSize:             "5G"
#HmssMillMaxConcurrency:          5
#FulfillmentResourceRequirements: ResourceRequirements=#ResourceRequirements & {
	requests: {
		cpu:    "200m"
		memory: "1Gi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}
#FulfillmentMaxHeapSize:             "640M"
#ControlServiceResourceRequirements: ResourceRequirements=#ResourceRequirements & {
	requests: {
		cpu:    "200m"
		memory: "512Mi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}
#ControlServiceMaxHeapSize: "192M"

objectSets: [defaultNetworkPolicies] + [ for objectSet in duchy {objectSet}]

_cloudStorageConfig: #CloudStorageConfig & {
	bucket: _cloudStorageBucket
}

duchy: #SpannerDuchy & {
	_duchy: {
		name:                   _duchy_name
		protocols_setup_config: _duchy_protocols_setup_config
		cs_cert_resource_name:  _duchy_cert_name
		if (_duchyKeyEncryptionKeyFile != _|_) {
			duchyKeyEncryptionKeyFile: _duchyKeyEncryptionKeyFile
		}
	}
	_duchy_secret_name: _secret_name
	_computation_control_targets: {
		"aggregator": _aggregatorSystemApiTarget
		"worker1":    _worker1SystemApiTarget
		"worker2":    _worker2SystemApiTarget
	}
	_kingdom_system_api_target:       #KingdomSystemApiTarget
	_kingdom_public_api_target:       #KingdomPublicApiTarget
	_trusteeMillSubnetworkRange:      _trusteeMillSubnetworkCidrRange
	_blob_storage_flags:              _cloudStorageConfig.flags
	_verbose_grpc_logging:            "false"
	_duchyMillParallelism:            4
	_liquidLegionsV2WorkLockDuration: "10m"

	serviceAccounts: {
		"\(#InternalServerServiceAccount)": #WorkloadIdentityServiceAccount & {
			_iamServiceAccountName: "\(_duchy_name)-duchy-internal"
		}
		"\(#StorageServiceAccount)": #WorkloadIdentityServiceAccount & {
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
				_javaOptions: maxHeapSize: #HeraldMaxHeapSize
				resources: #HeraldResourceRequirements
			}
			spec: template: spec: #ServiceAccountPodSpec & #SpotVmPodSpec & {
				serviceAccountName: #StorageServiceAccount
			}
		}
		"mill-job-scheduler-deployment": {
			_liquidLegionsV2MaxConcurrency: #Llv2MillMaxConcurrency
			_shareShuffleMaxConcurrency:    #HmssMillMaxConcurrency
		}
		"computation-control-server-deployment": {
			_container: {
				_javaOptions: maxHeapSize: #ControlServiceMaxHeapSize
				resources: #ControlServiceResourceRequirements
			}
			spec: template: spec: #ServiceAccountPodSpec & {
				serviceAccountName: #StorageServiceAccount
			}
		}
		"requisition-fulfillment-server-deployment": {
			_container: {
				_javaOptions: maxHeapSize: #FulfillmentMaxHeapSize
				resources: #FulfillmentResourceRequirements
			}
			spec: template: spec: #ServiceAccountPodSpec & {
				serviceAccountName: #StorageServiceAccount
			}
		}
	}

	services: {
		"requisition-fulfillment-server": _ipAddressName: _publicApiAddressName
		"computation-control-server": _ipAddressName:     _systemApiAddressName
		"internal-api-server": {
			metadata: annotations: {
				"cloud.google.com/load-balancer-type":          "Internal"
				"networking.gke.io/load-balancer-ip-addresses": _internalApiAddressName
			}
			if _trusteeMillSubnetworkCidrRange != _|_ {
				spec: {
					type: "LoadBalancer"
					loadBalancerSourceRanges: [
						_trusteeMillSubnetworkCidrRange,
					]
				}
			}
		}
	}

	podTemplates: {
		"llv2-mill": {
			_container: {
				_javaOptions: maxHeapSize: #Llv2MillMaxHeapSize
				resources: #Llv2MillResourceRequirements
			}
			template: spec: #ServiceAccountPodSpec & #SpotVmPodSpec & {
				serviceAccountName: #StorageServiceAccount
			}
		}
		"hmss-mill": {
			_container: {
				_javaOptions: maxHeapSize: #HmssMillMaxHeapSize
				resources: #HmssMillResourceRequirements
			}
			template: spec: #ServiceAccountPodSpec & #SpotVmPodSpec & {
				serviceAccountName: #StorageServiceAccount
			}
		}
	}
}
