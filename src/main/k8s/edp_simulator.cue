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

#EdpSimulator: {
	_edp: {display_name: string, resource_name: string}
	_mc_resource_name: string

	_edp_display_name:  _edp.display_name
	_edp_resource_name: _edp.resource_name

	_image:           string
	_imagePullPolicy: string
	_blob_storage_flags: [...string]

	apiVersion: "batch/v1"
	kind:       "Job"
	metadata: {
		name: "edp-simulator"
		labels: "app.kubernetes.io/name": #AppName
	}
	spec: template: spec: {
		containers: [{
			name:            "edp-simulator-container"
			image:           _image
			imagePullPolicy: _imagePullPolicy
			args:            [
						"--tls-cert-file=/var/run/secrets/files/\(_edp_display_name)_tls.pem",
						"--tls-key-file=/var/run/secrets/files/\(_edp_display_name)_tls.key",
						"--cert-collection-file=/var/run/secrets/files/all_root_certs.pem",
						"--public-api-protocol-configs=" + #PublicApiProtocolConfigs,
						"--data-provider-resource-name=\(_edp_resource_name)",
						"--measurement-consumer-resource-name=\(_mc_resource_name)",
						"--kingdom-public-api-target=" + (#Target & {name: "gcp-kingdom-data-server"}).target,
						"--kingdom-public-api-cert-host=localhost",
						"--requisition-fulfillment-service-target=" + (#Target & {name: "worker-1-requisition-fulfillment-server"}).target,
						"--requisition-fulfillment-service-cert-host=localhost",
			] + _blob_storage_flags
			volumeMounts: [{
				name:      "cache-volume"
				mountPath: "/cache"
			}]
		}]
		restartPolicy: "OnFailure"
		volumes: [{
			name: "cache-volume"
			emptyDir: {}
		}]
	}
}
