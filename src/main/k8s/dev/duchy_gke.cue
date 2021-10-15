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
_duchy_cert_name:              string @tag("duchy_cert_name")
_duchy_protocols_setup_config: string @tag("duchy_protocols_setup_config")
_secret_name:                  string @tag("secret_name")
_environment:                  string @tag("environment")

#KingdomSystemApiTarget:  "kingdom.dev.halo-cmm.org:8443"
#GloudProject:            "halo-cmm-dev"
#SpannerInstance:         "dev-instance"
#CloudStorageBucket:      "dev-bucket"
#ContainerRegistry:       "gcr.io"
#ContainerRegistryPrefix: #ContainerRegistry + "/" + #GloudProject

objectSets: [default_deny_ingress] + [ for d in duchy {d}]

duchy: #Duchy & {
	_env: _environment
	_duchy: {
		name:                   _duchy_name
		protocols_setup_config: _duchy_protocols_setup_config
		cs_cert_resource_name:  _duchy_cert_name
	}
	_duchy_secret_name:         _secret_name
	_kingdom_system_api_target: #KingdomSystemApiTarget
	_spanner_schema_push_flags: [
		"--ignore-already-existing-databases",
		"--instance-name=" + #SpannerInstance,
		"--project-name=" + #GloudProject,
	]
	_spanner_flags: [
		"--spanner-instance=" + #SpannerInstance,
		"--spanner-project=" + #GloudProject,
	]
	_blob_storage_flags: [
		"--google-cloud-storage-bucket=" + #CloudStorageBucket,
		"--google-cloud-storage-project=" + #GloudProject,
	]
	_images: {
		"async-computation-control-server": #ContainerRegistryPrefix + "/duchy/async-computation-control"
		"computation-control-server":       #ContainerRegistryPrefix + "/duchy/computation-control"
		"herald-daemon":                    #ContainerRegistryPrefix + "/duchy/herald"
		"liquid-legions-v2-mill-daemon":    #ContainerRegistryPrefix + "/duchy/liquid-legions-v2-mill"
		"requisition-fulfillment-server":   #ContainerRegistryPrefix + "/duchy/requisition-fulfillment"
		"push-spanner-schema-container":    #ContainerRegistryPrefix + "/setup/push-spanner-schema"
		"spanner-computations-server":      #ContainerRegistryPrefix + "/duchy/spanner-computations"
	}
	_duchy_image_pull_policy: "Always"
	_verbose_grpc_logging:    "false"
}
