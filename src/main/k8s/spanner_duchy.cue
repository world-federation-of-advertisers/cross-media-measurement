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

#SpannerDuchy: {
	#Duchy

	_duchy: _duchy
	_spannerConfig: #SpannerConfig & {
		database: "\(_duchy.name)_duchy_computations"
	}

	_imageSuffixes: {
		"\(_duchy_data_server_name)": string | *"duchy/spanner-computations"
		"update-spanner-duchy-schema": string | *"duchy/spanner-update-schema"
	}

	_duchy_data_server_name: "spanner-computations-server"
	_duchy_data_server_app_label: "spanner-computations-server-app"
	_duchy_data_server_deployment_name: "\(_duchy_data_server_name)-deployment"
	_duchy_data_server_container_args: _spannerConfig.flags
	_duchy_data_service_target_flag: "--computations-service-target=" + (#Target & {name: "\(_duchy.name)-\(_duchy_data_server_name)"}).target
	_duchy_data_service_cert_host_flag:  "--computations-service-cert-host=localhost"
	_duchy_update_schema_image: "update-spanner-duchy-schema"
}