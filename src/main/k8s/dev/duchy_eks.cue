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

_duchyName:                    string @tag("duchy_name")
_duchyProtocolsSetupConfig:    string @tag("duchy_protocols_setup_config")
_secretName:                   string @tag("secret_name")
_certificateId:                string @tag("certificate_id")
_computationControlServerEips: string @tag("computation_control_server_eips")
_aggregatorSystemApiTarget:    string @tag("aggregator_system_api_target")
_worker1SystemApiTarget:       string @tag("worker1_system_api_target")
_worker2SystemApiTarget:       string @tag("worker2_system_api_target")

_duchyCertName: "duchies/\(_duchyName)/certificates/\(_certificateId)"

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
		memory: "2.5Gi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}
#MillMaxHeapSize:        "1G"
#MillReplicas:           1
#FulfillmentMaxHeapSize: "96M"

objectSets: [
	default_deny_ingress_and_egress,
	duchy.deployments,
	duchy.services,
	duchy.networkPolicies,
	duchy.cronjobs,
]

duchy: #PostgresDuchy & {
	_imageSuffixes: {
		"computation-control-server":     "duchy/aws-computation-control"
		"liquid-legions-v2-mill-daemon":  "duchy/aws-liquid-legions-v2-mill"
		"requisition-fulfillment-server": "duchy/aws-requisition-fulfillment"
		"internal-api-server":            "duchy/aws-postgres-internal-server"
		"update-duchy-schema":            "duchy/aws-postgres-update-schema"
	}
	_duchy: {
		name:                   _duchyName
		protocols_setup_config: _duchyProtocolsSetupConfig
		cs_cert_resource_name:  _duchyCertName
	}
	_duchy_secret_name: _secretName
	_computation_control_targets: {
		"aggregator": _aggregatorSystemApiTarget
		"worker1":    _worker1SystemApiTarget
		"worker2":    _worker2SystemApiTarget
	}
	_kingdom_system_api_target: #KingdomSystemApiTarget
	_blob_storage_flags:        #AwsS3Config.flags
	_verbose_grpc_logging:      "false"
	_postgresConfig:            #AwsPostgresConfig
	services: {
		"computation-control-server": _eipAllocations: _computationControlServerEips
	}
	deployments: {
		"herald-daemon-deployment": {
			_container: {
				resources: #HeraldResourceRequirements
			}
			spec: template: spec: #PodSpec
		}
		"liquid-legions-v2-mill-daemon-deployment": {
			_workLockDuration: "10m"
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
			_container: {
				_javaOptions: maxHeapSize: #FulfillmentMaxHeapSize
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
}
