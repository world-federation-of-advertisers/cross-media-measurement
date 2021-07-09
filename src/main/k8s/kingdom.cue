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

package k8s

import ("strings")

#Kingdom: {
	_verbose_grpc_logging: "true" | "false"

	_spanner_schema_push_flags: [...string]
	_spanner_flags: [...string]

	_images: [Name=_]: string
	_kingdom_image_pull_policy: string

	_duchy_info_config_flag:                 "--duchy-info-config=" + #DuchyInfoConfig
	_debug_verbose_grpc_client_logging_flag: "--debug-verbose-grpc-client-logging=\(_verbose_grpc_logging)"
	_debug_verbose_grpc_server_logging_flag: "--debug-verbose-grpc-server-logging=\(_verbose_grpc_logging)"

	kingdom_service: [Name=_]: #GrpcService & {
		_name:   Name
		_system: "kingdom"
	}

	kingdom_service: {
		"gcp-kingdom-data-server": {}
		"requisition-server": {}
		"system-api-server": {}
	}

	kingdom_job: "kingdom-push-spanner-schema-job": {
		apiVersion: "batch/v1"
		kind:       "Job"
		metadata: {
			name: "kingdom-push-spanner-schema-job"
			labels: "app.kubernetes.io/name": #AppName
		}
		spec: template: spec: {
			containers: [{
				name:            "push-spanner-schema-container"
				image:           _images[name]
				imagePullPolicy: _kingdom_image_pull_policy
				args:            [
							"--databases=kingdom=/app/wfa_measurement_system/src/main/kotlin/org/wfanet/measurement/kingdom/deploy/gcloud/spanner/kingdom_legacy.sdl",
				] + _spanner_schema_push_flags
			}]
			restartPolicy: "OnFailure"
		}
	}

	kingdom_pod: [Name=_]: #Pod & {
		_name:            strings.TrimSuffix(Name, "-pod")
		_system:          "kingdom"
		_image:           _images[_name]
		_imagePullPolicy: _kingdom_image_pull_policy
	}

	kingdom_pod: {

		"gcp-kingdom-data-server-pod": #ServerPod & {
			_args: [
				_duchy_info_config_flag,
				_debug_verbose_grpc_server_logging_flag,
				"--port=8080",
			] + _spanner_flags
		}

		"system-api-server-pod": #ServerPod & {
			_args: [
				_debug_verbose_grpc_client_logging_flag,
				_debug_verbose_grpc_server_logging_flag,
				_duchy_info_config_flag,
				"--internal-api-target=" + (#Target & {name: "gcp-kingdom-data-server"}).target,
				"--port=8080",
			]
			_dependencies: ["gcp-kingdom-data-server"]
		}
	}
}
