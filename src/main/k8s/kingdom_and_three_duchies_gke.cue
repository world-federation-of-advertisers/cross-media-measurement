// Copyright 2020 The Cross-Media Measurement Authors
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

// cue cmd dump src/main/k8s/kingdom_and_three_duchies_from_cue.cue >
// src/main/k8s/kingdom_and_three_duchies_from_cue.yaml

package k8s

_spanner_project:           string @tag("spanner_project")
_spanner_instance:          string @tag("spanner_instance")
_cloud_storage_project:     string @tag("cloud_storage_project")
_cloud_storage_bucket:      string @tag("cloud_storage_bucket")
_container_registry:        string @tag("container_registry")
_repository_prefix:         string @tag("repository_prefix")
_container_registry_prefix: _container_registry + "/" + _repository_prefix

objectSets: [
		kingdom.kingdom_service,
		kingdom.kingdom_pod,
		kingdom.kingdom_job,
] + [ for d in duchies for v in d {v}]

#Duchies: [
	{
		name:                   "aggregator"
		protocols_setup_config: #AggregatorProtocolsSetupConfig
		tls_cert_file:          "/var/run/secrets/files/aggregator.pem"
		tls_key_file:           "/var/run/secrets/files/aggregator.key"
		cert_collection_file:   "/var/run/secrets/files/all_root_certs.pem"
	},
	{
		name:                   "worker-1"
		protocols_setup_config: #NonAggregatorProtocolsSetupConfig
		tls_cert_file:          "/var/run/secrets/files/worker_1.pem"
		tls_key_file:           "/var/run/secrets/files/worker_1.key"
		cert_collection_file:   "/var/run/secrets/files/all_root_certs.pem"
	},
	{
		name:                   "worker-2"
		protocols_setup_config: #NonAggregatorProtocolsSetupConfig
		tls_cert_file:          "/var/run/secrets/files/worker_2.pem"
		tls_key_file:           "/var/run/secrets/files/worker_2.key"
		cert_collection_file:   "/var/run/secrets/files/all_root_certs.pem"
	},
]

#GkeDuchy: #Duchy & {
	_aggregator_name: "duchy-aggregator"
	_spanner_schema_push_flags: [
		"--ignore-already-existing-databases",
		"--instance-name=\(_spanner_instance)",
		"--project-name=\(_spanner_project)",
	]
	_spanner_flags: [
		"--spanner-instance=\(_spanner_instance)",
		"--spanner-project=\(_spanner_project)",
	]
	_blob_storage_flags: [
		"--google-cloud-storage-bucket=\(_cloud_storage_bucket)",
		"--google-cloud-storage-project=\(_cloud_storage_project)",
	]
	_images: {
		"async-computation-control-server": "\(_container_registry_prefix)/duchy/async-computation-control"
		"computation-control-server":       "\(_container_registry_prefix)/duchy/computation-control"
		"herald-daemon":                    "\(_container_registry_prefix)/duchy/herald"
		"liquid-legions-v2-mill-daemon":    "\(_container_registry_prefix)/duchy/liquid-legions-v2-mill"
		"requisition-fulfillment-server":   "\(_container_registry_prefix)/duchy/requisition-fulfillment"
		"push-spanner-schema-container":    "\(_container_registry_prefix)/setup/push-spanner-schema"
		"spanner-computations-server":      "\(_container_registry_prefix)/duchy/spanner-computations"
	}
	_duchy_image_pull_policy: "Always"
	_verbose_grpc_logging:    "false"
}

duchies: {for d in #Duchies {"\(d.name)": #GkeDuchy & {_duchy: d}}}

kingdom: #Kingdom & {
	_duchy_ids: [ for d in #Duchies {"duchy-\(d.name)"}]
	_spanner_schema_push_flags: [
		"--ignore-already-existing-databases",
		"--instance-name=\(_spanner_instance)",
		"--project-name=\(_spanner_project)",
	]
	_spanner_flags: [
		"--spanner-database=kingdom",
		"--spanner-instance=\(_spanner_instance)",
		"--spanner-project=\(_spanner_project)",
	]
	_images: {
		"push-spanner-schema-container": "\(_container_registry_prefix)/setup/push-spanner-schema"
		"gcp-kingdom-data-server":       "\(_container_registry_prefix)/kingdom/data-server"
		"system-api-server":             "\(_container_registry_prefix)/kingdom/system-api"
	}
	_kingdom_image_pull_policy: "Always"
	_verbose_grpc_logging:      "false"
}
