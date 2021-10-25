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
_secret_name: string @tag("secret_name")

#KingdomPublicApiTarget:  "public.kingdom.dev.halo-cmm.org:8443"
#GloudProject:            "halo-cmm-dev"
#CloudStorageBucket:      "halo-cmm-dev-bucket"
#ContainerRegistry:       "gcr.io"
#ContainerRegistryPrefix: #ContainerRegistry + "/" + #GloudProject

objectSets: [frontend_simulator]

frontend_simulator: #FrontendSimulator & {
	_mc_resource_name:          _mc_name
	_mc_secret_name:            _secret_name
	_kingdom_public_api_target: #KingdomPublicApiTarget
	_simulator_image:           #ContainerRegistryPrefix + "/loadtest/frontend-simulator"
	_blob_storage_flags: [
		"--google-cloud-storage-bucket=" + #CloudStorageBucket,
		"--google-cloud-storage-project=" + #GloudProject,
	]
}
