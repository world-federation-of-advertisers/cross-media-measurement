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

_mc_name:   string @tag("mc_name")
_edp1_name: string @tag("edp1_name")
_edp2_name: string @tag("edp2_name")
_edp3_name: string @tag("edp3_name")
_edp4_name: string @tag("edp4_name")
_edp5_name: string @tag("edp5_name")
_edp6_name: string @tag("edp6_name")
_edpResourceNames: [_edp1_name, _edp2_name, _edp3_name, _edp4_name, _edp5_name, _edp6_name]
_secret_name:        string @tag("secret_name")
_cloudStorageBucket: string @tag("cloud_storage_bucket")

// #KingdomPublicApiTarget: "public.kingdom.dev.halo-cmm.org:8443"
// #DuchyPublicApiTarget:   "public.worker1.dev.halo-cmm.org:8443"
#BigQueryDataSet:        "demo"
#BigQueryTable:          "labelled_events"
#ServiceAccount:         "simulator"

// DNS name of the public API of the Kingdom.  This is used to poll for
// new requisition requests.  The default is to look for a Kingdom running
// in the same cluster.  For a multi-cluster deployment, this should be set
// to the fully qualified domain name of the server that is running the
// v2alpha-public-api-server service.
//
// Example using fully qualified domain name:
// #KingdomPublicApiTarget: "public.kingdom.dev.halo-cmm.org:8443"
#KingdomPublicApiTarget:    "v2alpha-public-api-server:8443"

// DNS name of the public API provided by the duchies.  When an EDP fulfills
// a requisition, the fulfilled requisition is sent to this server.
// The default is to look for a Duchy running in the same cluster.  For
// a multi-cluster deployment, this should be set to the fully qualified
// domain name of the server that is running the worker1-requisition-fulfillment-server
// service.
#DuchyPublicApiTarget:      "worker1-requisition-fulfillment-server:8443"

// #BigQueryTableName:         "demo.labelled_events"

// EDP simulators need to be able to access BigQuery since that is
// where all of the event data is stored.
#StorageServiceAccount:     "storage"

#EdpSimulatorResourceConfig: {
	replicas: 1
	resources: {
		requests: {
			cpu: "100m"
		}
		limits: {
			cpu:    "400m"
			memory: "512Mi"
		}
	}
	jvmHeapSize: "400m"
}

objectSets: [ for edp in edp_simulators {edp}] + network_policies

_cloudStorageConfig: #CloudStorageConfig & {
	bucket: _cloudStorageBucket
}
_bigQueryConfig: #BigQueryConfig & {
	dataset: #BigQueryDataSet
	table:   #BigQueryTable
}
_imageConfig: #ImageConfig & {
	repoSuffix: "loadtest/edp-simulator"
}

#EdpConfig: {
	publisherId: int
}
_edpConfigs: [...#EdpConfig]
_edpConfigs: [
	for i, name in _edpResourceNames {
		publisherId:  i + 1
		resourceName: name
		displayName:  "edp\(publisherId)"
	},
]

edp_simulators: {
	for edp in _edpConfigs {
		"\(edp.displayName)": #EdpSimulator & {
			_edpConfig:                   edp
			_edp_secret_name:             _secret_name
			_duchy_public_api_target:     #DuchyPublicApiTarget
			_kingdom_public_api_target:   #KingdomPublicApiTarget
			_blob_storage_flags:          _cloudStorageConfig.flags
			_mc_resource_name:            _mc_name
			_edp_simulator_image:         _imageConfig.image
			_simulator_image_pull_policy: "Always"
			_additional_args:             ["--publisher-id=\(edp.publisherId)"] + _bigQueryConfig.flags
			deployment: spec: template: spec: #ServiceAccountPodSpec & {
				serviceAccountName: #ServiceAccount
			}
		}
	}
}

network_policies: {
        [for edp in #Edps {
		networkPolicies: #NetworkPolicy & {
       			_name: 	 	edp.display_name
			_app_label: 	edp.display_name + "-simulator-app"
			_egresses: {
				      // Need to send external traffic.
			  	      any: {}
			}
		}
	}]
}

