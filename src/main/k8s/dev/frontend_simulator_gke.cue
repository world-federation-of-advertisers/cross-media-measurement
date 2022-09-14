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

_mc_name:            string @tag("mc_name")
_mc_api_key:         string @tag("mc_api_key")
_secret_name:        string @tag("secret_name")
_cloudStorageBucket: string @tag("cloud_storage_bucket")

#KingdomPublicApiTarget: "public.kingdom.dev.halo-cmm.org:8443"
#ServiceAccount:         "simulator"

objectSets: [frontend_simulator]

_cloudStorageConfig: #CloudStorageConfig & {
	bucket: _cloudStorageBucket
}
_imageConfig: #ImageConfig & {
	repoSuffix: "loadtest/frontend-simulator"
}

frontend_simulator: #FrontendSimulator & {
	_mc_resource_name:          _mc_name
	_mc_api_authentication_key: _mc_api_key
	_mc_secret_name:            _secret_name
	_kingdom_public_api_target: #KingdomPublicApiTarget
	_simulator_image:           _imageConfig.image
	_blob_storage_flags:        _cloudStorageConfig.flags
	job: spec: template: spec: #ServiceAccountPodSpec & {
		serviceAccountName: #ServiceAccount
	}
}
