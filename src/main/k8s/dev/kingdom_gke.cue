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

_secret_name:          string @tag("secret_name")
_publicApiAddressName: string @tag("public_api_address_name")
_systemApiAddressName: string @tag("system_api_address_name")

// Name of K8s service account for the internal API server.
#InternalServerServiceAccount: "internal-server"

// Name of K8s service account for the operational metrics job.
#OperationalMetricsServiceAccount: "operational-metrics"

// Number of gRPC threads for the internal API server.
#InternalServerGrpcThreads: 7

// Number of gRPC threads for the system API server.
//
// This serves long-lived streaming RPCs from each Herald which will each occupy
// a thread, so this should be greater than the number of Heralds.
#SystemServerGrpcThreads: 5

#InternalServerResourceRequirements: ResourceRequirements=#ResourceRequirements & {
	requests: {
		cpu:    "500m"
		memory: "352Mi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}

#OperationalMetricsJobResourceRequirements: ResourceRequirements=#ResourceRequirements & {
	requests: {
		cpu:    "10m"
		memory: "256Mi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}

objectSets: [defaultNetworkPolicies] + [ for objectSet in kingdom {objectSet}]

kingdom: #Kingdom & {
	_kingdom_secret_name: _secret_name
	_spannerConfig: database: "kingdom"

	_verboseGrpcServerLogging: true

	_imageSuffixes: [string]: string
	_imageSuffixes: {
		"operational-metrics": string | *"kingdom/bigquery-operational-metrics"
	}
	_imageConfigs: [string]: #ImageConfig
	_imageConfigs: {
		for name, suffix in _imageSuffixes {
			"\(name)": {repoSuffix: suffix}
		}
	}
	_images: [string]: string
	_images: {
		for name, config in _imageConfigs {
			"\(name)": config.image
		}
	}

	serviceAccounts: {
		"\(#InternalServerServiceAccount)": #WorkloadIdentityServiceAccount & {
			_iamServiceAccountName: "kingdom-internal"
		}
		"\(#OperationalMetricsServiceAccount)": #WorkloadIdentityServiceAccount & {
			_iamServiceAccountName: "operational-metrics"
		}
	}

	configMaps: "java": #JavaConfigMap

	deployments: {
		"gcp-kingdom-data-server": {
			_container: {
				_grpcThreadPoolSize: #InternalServerGrpcThreads
				resources:           #InternalServerResourceRequirements
			}
			spec: template: spec: #ServiceAccountPodSpec & {
				serviceAccountName: #InternalServerServiceAccount
			}
		}
		"system-api-server": {
			_container: {
				_grpcThreadPoolSize: #SystemServerGrpcThreads
			}
		}
	}

	cronJobs: {
		"operational-metrics": {
			_container: {
				_javaOptions: maxHeapSize: "48M"
				image:     _images["operational-metrics"]
				resources: #OperationalMetricsJobResourceRequirements
				args: [
					"--bigquery-project=\(#GCloudProject)",
					"--bigquery-dataset=operational_metrics",
					"--measurements-table=measurements",
					"--latest-measurement-read-table=latest_measurement_read",
					"--requisitions-table=requisitions",
					"--latest-requisition-read-table=latest_requisition_read",
					"--computation-participant-stages-table=computation_participant_stages",
					"--latest-computation-read-table=latest_computation_read",
					"--batch-size=1000",
					"--tls-cert-file=/var/run/secrets/files/kingdom_tls.pem",
					"--tls-key-file=/var/run/secrets/files/kingdom_tls.key",
					"--cert-collection-file=/var/run/secrets/files/kingdom_root.pem",
					"--internal-api-target=" + (#Target & {name: "gcp-kingdom-data-server"}).target,
					"--internal-api-cert-host=localhost",
				]
			}
			spec: {
				concurrencyPolicy: "Forbid"
				schedule:          "30 * * * *" // Hourly, 30 minutes past the hour
				jobTemplate: spec: template: spec: #ServiceAccountPodSpec & {
					serviceAccountName: #OperationalMetricsServiceAccount
				}
			}
		}
	}

	networkPolicies: {
		"operational-metrics": {
			_app_label: "operational-metrics-app"
			_egresses: {
				// Need to send external traffic to BigQuery.
				any: {}
			}
		}
	}

	services: {
		"system-api-server": _ipAddressName:         _systemApiAddressName
		"v2alpha-public-api-server": _ipAddressName: _publicApiAddressName
	}
}
