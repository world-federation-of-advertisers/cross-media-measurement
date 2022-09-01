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

// DNS name of the public API of the Kingdom.  The default is to look for a 
// Kingdom running in the same cluster.  For a multi-cluster deployment, this
// should be set to the fully qualified domain name of the server that is 
// running the v2alpha-public-api-server service.
//
// Example using fully qualified domain name:
// #KingdomPublicApiTarget: "public.kingdom.dev.halo-cmm.org:8443"
#KingdomPublicApiTarget:    "v2alpha-public-api-server:8443"

// The storage bucket associated with the frontend simulator must be
// writable by this account.
#ServiceAccount: "internal-server"

objectSets: [frontend_simulator, [network_policy]]

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

// Allow front-end simulator to communicate with other nodes in cluster.
network_policy: #NetworkPolicy & {
      _name:          "frontend-simulator"
      _app_label:     "frontend-simulator-app"
      _egresses: {
                 // Need to send external traffic.
                 any: {}
       }
}
