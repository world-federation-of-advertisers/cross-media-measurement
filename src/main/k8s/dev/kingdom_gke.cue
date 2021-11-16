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

_secret_name: string @tag("secret_name")
_environment: string @tag("environment")

#GloudProject:            "halo-cmm-dev"
#SpannerInstance:         "dev-instance"
#ContainerRegistry:       "gcr.io"
#ContainerRegistryPrefix: #ContainerRegistry + "/" + #GloudProject
#DefaultResourceConfig: {
	replicas:              1
	resourceRequestCpu:    "100m"
	resourceLimitCpu:      "400m"
	resourceRequestMemory: "256Mi"
	resourceLimitMemory:   "512Mi"
}

objectSets: [
		// resource_setup_job, Only deploy if the kingdom database is reset.
		default_deny_ingress_and_egress,
] + [ for k in kingdom {k}]

kingdom: #Kingdom & {
	_env:                 _environment
	_kingdom_secret_name: _secret_name
	_spanner_schema_push_flags: [
		"--ignore-already-existing-databases",
		"--instance-name=" + #SpannerInstance,
		"--project-name=" + #GloudProject,
	]
	_spanner_flags: [
		"--spanner-database=kingdom",
		"--spanner-instance=" + #SpannerInstance,
		"--spanner-project=" + #GloudProject,
	]
	_images: {
		"push-spanner-schema-container": #ContainerRegistryPrefix + "/setup/push-spanner-schema"
		"gcp-kingdom-data-server":       #ContainerRegistryPrefix + "/kingdom/data-server"
		"system-api-server":             #ContainerRegistryPrefix + "/kingdom/system-api"
		"v2alpha-public-api-server":     #ContainerRegistryPrefix + "/kingdom/v2alpha-public-api"
	}
	_resource_configs: {
		"gcp-kingdom-data-server":   #DefaultResourceConfig
		"system-api-server":         #DefaultResourceConfig
		"v2alpha-public-api-server": #DefaultResourceConfig
	}
	_kingdom_image_pull_policy: "Always"
	_verbose_grpc_logging:      "false"
}

resource_setup_job: #ResourceSetup & {
	_edp_display_names: ["edp1", "edp2", "edp3", "edp4", "edp5", "edp6"]
	_duchy_ids: ["aggregator", "worker1", "worker2"]
	_job_image:                  #ContainerRegistryPrefix + "/loadtest/resource-setup"
	_resource_setup_secret_name: _secret_name
}
