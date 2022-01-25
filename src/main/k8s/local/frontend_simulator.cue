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

_mc_name:     string @tag("mc_name")
_mc_api_key:  string @tag("mc_api_key")
_secret_name: string @tag("secret_name")

#KingdomPublicApiTarget: (#Target & {name: "v2alpha-public-api-server"}).target

objectSets: [frontendSimulator]

frontendSimulator: #FrontendSimulator & {
	_mc_resource_name:            _mc_name
	_mc_secret_name:              _secret_name
	_mc_api_authentication_key:   _mc_api_key
	_kingdom_public_api_target:   #KingdomPublicApiTarget
	_simulator_image:             "bazel/src/main/kotlin/org/wfanet/measurement/loadtest/frontend:forwarded_storage_frontend_simulator_runner_image"
	_simulator_image_pull_policy: "Never"
	_blob_storage_flags: [
		"--forwarded-storage-service-target=" + (#Target & {name: "fake-storage-server"}).target,
		"--forwarded-storage-cert-host=localhost",
	]
}
