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

objectSets: [resourceSetup]

resourceSetup: #ResourceSetup & {
	_edp_display_names: ["edp1", "edp2", "edp3", "edp4", "edp5", "edp6"]
	_duchy_ids: ["aggregator", "worker1", "worker2"]
	_job_image:                  "bazel/src/main/kotlin/org/wfanet/measurement/loadtest/resourcesetup:resource_setup_runner_image"
	_job_image_pull_policy:      "Never"
	_resource_setup_secret_name: _secret_name
	_dependencies: ["gcp-kingdom-data-server"]
}
