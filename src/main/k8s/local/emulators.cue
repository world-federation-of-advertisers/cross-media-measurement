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

objectSets: [
	services,
	pods,
	deployments,
]

services: {
	"spanner-emulator": {
		apiVersion: "v1"
		kind:       "Service"
		metadata: {
			name: "spanner-emulator"
			labels: {
				"app.kubernetes.io/part-of":   #AppName
				"app.kubernetes.io/component": "testing"
			}
		}
		spec: {
			selector: app: "spanner-emulator-app"
			ports: [{
				name:       "grpc"
				port:       9010
				protocol:   "TCP"
				targetPort: 9010
			}, {
				name:       "http"
				port:       9020
				protocol:   "TCP"
				targetPort: 9020
			}]
		}
	}
	"fake-storage-server": #GrpcService & {
		_name:   "fake-storage-server"
		_system: "testing"
	}
}

pods: {
	"spanner-emulator-pod": {
		apiVersion: "v1"
		kind:       "Pod"
		metadata: {
			name: "spanner-emulator-pod"
			labels: {
				app:                           "spanner-emulator-app"
				"app.kubernetes.io/part-of":   #AppName
				"app.kubernetes.io/component": "testing"
			}
		}
		spec: containers: [{
			name:  "spanner-emulator-container"
			image: "gcr.io/cloud-spanner-emulator/emulator"
		}]
	}
}

deployments: {
	"fake-storage-server-deployment": #ServerDeployment & {
		_name:       "fake-storage-server"
		_secretName: _secret_name
		_image:      "bazel/src/main/kotlin/org/wfanet/measurement/storage/filesystem:server_image"
		_system:     "testing"
		_args: [
			"--debug-verbose-grpc-server-logging=true",
			"--port=8443",
			"--require-client-auth=false",
			"--tls-cert-file=/var/run/secrets/files/kingdom_tls.pem",
			"--tls-key-file=/var/run/secrets/files/kingdom_tls.key",
			"--cert-collection-file=/var/run/secrets/files/all_root_certs.pem",
		]
		_resourceConfig: #DefaultResourceConfig & {
			resources: {
				limits: {
					cpu:    cpu | *"400m"
					memory: "1Gi"
				}
			}
		}
	}
}
