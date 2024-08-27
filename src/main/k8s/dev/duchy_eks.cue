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

_duchyName:                 string @tag("duchy_name")
_duchyProtocolsSetupConfig: string @tag("duchy_protocols_setup_config")
_secretName:                string @tag("secret_name")
_certificateId:             string @tag("certificate_id")
_publicApiEipAllocs:        string @tag("public_api_eip_allocs")
_systemApiEipAllocs:        string @tag("system_api_eip_allocs")
_aggregatorSystemApiTarget: string @tag("aggregator_system_api_target")
_worker1SystemApiTarget:    string @tag("worker1_system_api_target")
_worker2SystemApiTarget:    string @tag("worker2_system_api_target")
_duchyKeyEncryptionKeyFile: string @tag("duchy_key_encryption_key_file")

_duchyCertName: "duchies/\(_duchyName)/certificates/\(_certificateId)"

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
		memory: "576Mi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}
#FulfillmentMaxHeapSize:             "320M"
#ControlServiceResourceRequirements: ResourceRequirements=#ResourceRequirements & {
	requests: {
		cpu:    "200m"
		memory: "512Mi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}
#ControlServiceMaxHeapSize: "320M"

objectSets: [defaultNetworkPolicies] + [ for objectSet in duchy {objectSet}]

duchy: #PostgresDuchy & {
	_imageSuffixes: {
		"herald-daemon":                  "duchy/aws-herald"
		"computation-control-server":     "duchy/aws-computation-control"
		"llv2-mill":                      "duchy/aws-liquid-legions-v2-mill"
		"hmss-mill":                      "duchy/aws-honest-majority-share-shuffle-mill"
		"requisition-fulfillment-server": "duchy/aws-requisition-fulfillment"
		"internal-api-server":            "duchy/aws-postgres-internal-server"
		"update-duchy-schema":            "duchy/aws-postgres-update-schema"
	}
	_duchy: {
		name:                      _duchyName
		protocols_setup_config:    _duchyProtocolsSetupConfig
		cs_cert_resource_name:     _duchyCertName
		duchyKeyEncryptionKeyFile: _duchyKeyEncryptionKeyFile
	}
	_duchy_secret_name: _secretName
	_computation_control_targets: {
		"aggregator": _aggregatorSystemApiTarget
		"worker1":    _worker1SystemApiTarget
		"worker2":    _worker2SystemApiTarget
	}
	_kingdom_system_api_target:       #KingdomSystemApiTarget
	_kingdom_public_api_target:       #KingdomPublicApiTarget
	_blob_storage_flags:              #AwsS3Config.flags
	_verbose_grpc_logging:            "false"
	_duchyMillParallelism:            4
	_liquidLegionsV2WorkLockDuration: "10m"
	_postgresConfig:                  #AwsPostgresConfig

	services: {
		"requisition-fulfillment-server": _eipAllocations: _publicApiEipAllocs
		"computation-control-server": _eipAllocations:     _systemApiEipAllocs
	}
	deployments: {
		"herald-daemon-deployment": {
			_container: {
				_javaOptions: maxHeapSize: #HeraldMaxHeapSize
				resources: #HeraldResourceRequirements
			}
			spec: template: spec: #ServiceAccountPodSpec & {
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
		"internal-api-server-deployment": {
			_container: {
				resources: #InternalServerResourceRequirements
			}
			spec: template: spec: #ServiceAccountPodSpec & {
				serviceAccountName: #InternalServerServiceAccount
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
