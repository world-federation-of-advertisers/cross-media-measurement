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
_duchy_protocols_setup_config: string @tag("duchy_protocols_setup_config")
_secret_name:                  string @tag("secret_name")
_cloudStorageBucket:           string @tag("cloud_storage_bucket")
_certificateId:                string @tag("certificate_id")

_duchy_cert_name: "duchies/\(_duchy_name)/certificates/\(_certificateId)"

#KingdomSystemApiTarget:       "system.kingdom.dev.halo-cmm.org:8443"
#InternalServerServiceAccount: "internal-server"
#StorageServiceAccount:        "storage"
#DuchyServerResourceConfig:    #DefaultResourceConfig & {
}
#MillResourceConfig: #DefaultResourceConfig & {
	replicas: 1
	resources: {
		requests: {
			cpu: "200m"
		}
		limits: {
			cpu:    "800m"
			memory: "4Gi"
		}
	}
	jvmHeapSize: "3584m"
}
#HeraldResourceConfig: #DefaultResourceConfig & {
	replicas: 1 // We should have 1 and only 1 herald.
}

objectSets: [
	default_deny_ingress_and_egress,
	duchy.deployments,
	duchy.services,
	duchy.networkPolicies,
]

_cloudStorageConfig: #CloudStorageConfig & {
	bucket: _cloudStorageBucket
}

_imageSuffixes: [_=string]: string
_imageSuffixes: {
	"async-computation-control-server": "duchy/async-computation-control"
	"computation-control-server":       "duchy/computation-control"
	"herald-daemon":                    "duchy/herald"
	"liquid-legions-v2-mill-daemon":    "duchy/liquid-legions-v2-mill"
	"requisition-fulfillment-server":   "duchy/requisition-fulfillment"
	"spanner-computations-server":      "duchy/spanner-computations"
	"update-duchy-schema":              "duchy/spanner-update-schema"
}
_imageConfigs: [_=string]: #ImageConfig
_imageConfigs: {
	for name, suffix in _imageSuffixes {
		"\(name)": {repoSuffix: suffix}
	}
}

duchy: #Duchy & {
	_duchy: {
		name:                   _duchy_name
		protocols_setup_config: _duchy_protocols_setup_config
		cs_cert_resource_name:  _duchy_cert_name
	}
	_duchy_secret_name: _secret_name
	_computation_control_targets: {
		"aggregator": "system.aggregator.dev.halo-cmm.org:8443"
		"worker1":    "system.worker1.dev.halo-cmm.org:8443"
		"worker2":    "system.worker2.dev.halo-cmm.org:8443"
	}
	_kingdom_system_api_target: #KingdomSystemApiTarget
	_blob_storage_flags:        _cloudStorageConfig.flags
	_images: {
		for name, config in _imageConfigs {
			"\(name)": config.image
		}
	}
	_resource_configs: {
		"async-computation-control-server": #DuchyServerResourceConfig
		"computation-control-server":       #DuchyServerResourceConfig
		"herald-daemon":                    #HeraldResourceConfig
		"liquid-legions-v2-mill-daemon":    #MillResourceConfig
		"requisition-fulfillment-server":   #DuchyServerResourceConfig
		"spanner-computations-server":      #DuchyServerResourceConfig
	}
	_duchy_image_pull_policy: "Always"
	_verbose_grpc_logging:    "false"

	deployments: {
		"spanner-computations-server-deployment": {
			_podSpec: #ServiceAccountPodSpec & {
				serviceAccountName: #InternalServerServiceAccount
			}
		}
		"liquid-legions-v2-mill-daemon-deployment": {
			_podSpec: #ServiceAccountPodSpec & {
				serviceAccountName: #StorageServiceAccount
			}
		}
		"computation-control-server-deployment": {
			_podSpec: #ServiceAccountPodSpec & {
				serviceAccountName: #StorageServiceAccount
			}
		}
		"requisition-fulfillment-server-deployment": {
			_podSpec: #ServiceAccountPodSpec & {
				serviceAccountName: #StorageServiceAccount
			}
		}
	}
}
