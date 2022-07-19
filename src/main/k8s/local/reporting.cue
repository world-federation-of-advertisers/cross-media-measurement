// Copyright 2022 The Cross-Media Measurement Authors
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

#DefaultResourceConfig: {
	replicas:              1
	resourceRequestCpu:    "100m"
	resourceLimitCpu:      "400m"
	resourceRequestMemory: "256Mi"
	resourceLimitMemory:   "512Mi"
}

objectSets: [ for objectSet in reporting {objectSet}]

reporting: #Reporting & {
	_reporting_secret_name: _secret_name
	_postgresConfig: {
	  database: "reporting"
	  host: "localhost"
	  port: "9040",
	  user: "user",
	  password: "password",
	}
	_images: {
		"postgres-reporting-data-server":   "bazel/src/main/kotlin/org/wfanet/measurement/reporting/deploy/postgres/server:postgres_reporting_data_server_image"
		"v1alpha-public-api-server": "bazel/src/main/kotlin/org/wfanet/measurement/reporting/deploy/common/server:v1alpha_public_api_server_image"
	}
	_resource_configs: {
		"postgres-reporting-data-server":   #DefaultResourceConfig
		"v1alpha-public-api-server": #DefaultResourceConfig
	}
	_reporting_image_pull_policy: "Never"
	_verboseGrpcServerLogging:  true
	_verboseGrpcClientLogging:  true
}
