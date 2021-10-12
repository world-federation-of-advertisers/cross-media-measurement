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

// Group 1
objectSets: [
	resource_setup_job,
	default_deny_ingress,
	kingdom.kingdom_service,
	kingdom.kingdom_deployment,
	kingdom.kingdom_job,
	kingdom.kingdom_internal_network_policies,
]

// Group 2
//objectSets: [ for d in duchies for v in d {v}] + [ for d in edp_simulators {d}]

// Group 3
//objectSets: [
// frontend_simulator,
//]

#SecretName: "TBD"

#Edps: [
	{
		display_name:  "edp1"
		resource_name: "TBD"
	},
	{
		display_name:  "edp2"
		resource_name: "TBD"
	},
	{
		display_name:  "edp3"
		resource_name: "TBD"
	},
	{
		display_name:  "edp4"
		resource_name: "TBD"
	},
	{
		display_name:  "edp5"
		resource_name: "TBD"
	},
	{
		display_name:  "edp6"
		resource_name: "TBD"
	},
]

#McResourcename: "TBD"

#Duchies: [
	{
		name:                   "aggregator"
		protocols_setup_config: "aggregator_protocols_setup_config.textproto"
		cs_cert_resource_name:  "TBD"
	},
	{
		name:                   "worker1"
		protocols_setup_config: "nonaggregator_protocols_setup_config.textproto"
		cs_cert_resource_name:  "TBD"
	},
	{
		name:                   "worker2"
		protocols_setup_config: "nonaggregator_protocols_setup_config.textproto"
		cs_cert_resource_name:  "TBD"
	},
]

#KingdomPublicApiTarget: (#Target & {name: "v2alpha-public-api-server"}).target
#KingdomSystemApiTarget: (#Target & {name: "system-api-server"}).target
#Worker1PublicApiTarget: (#Target & {name: "worker1-requisition-fulfillment-server"}).target

#GkeDuchy: #Duchy & {
	_duchy_secret_name:         #SecretName
	_kingdom_system_api_target: #KingdomSystemApiTarget
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
	_kingdom_secret_name: #SecretName
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
		"v2alpha-public-api-server":     "\(_container_registry_prefix)/kingdom/v2alpha-public-api"
	}
	_kingdom_image_pull_policy: "Always"
	_verbose_grpc_logging:      "false"
}

frontend_simulator: #FrontendSimulator & {
	_mc_resource_name:          #McResourcename
	_kingdom_public_api_target: #KingdomPublicApiTarget
	_simulator_image:           "\(_container_registry_prefix)/loadtest/frontend-simulator"
	_blob_storage_flags: [
		"--google-cloud-storage-bucket=\(_cloud_storage_bucket)",
		"--google-cloud-storage-project=\(_cloud_storage_project)",
	]
}

resource_setup_job: #ResourceSetup & {
	_edp_display_names: [ for d in #Edps {d.display_name}]
	_duchy_ids: [ for d in #Duchies {d.name}]
	_job_image:                  "\(_container_registry_prefix)/loadtest/resource-setup"
	_resource_setup_secret_name: #SecretName
}

edp_simulators: {
	for d in #Edps {
		"\(d.display_name)": #EdpSimulator & {
			_edp:                       d
			_edp_secret_name:           #SecretName
			_duchy_public_api_target:   #Worker1PublicApiTarget
			_kingdom_public_api_target: #KingdomPublicApiTarget
			_blob_storage_flags: [
				"--google-cloud-storage-bucket=\(_cloud_storage_bucket)",
				"--google-cloud-storage-project=\(_cloud_storage_project)",
			]
			_mc_resource_name:            #McResourcename
			_edp_simulator_image:         "\(_container_registry_prefix)/loadtest/edp-simulator"
			_simulator_image_pull_policy: "Always"
		}
	}
}
