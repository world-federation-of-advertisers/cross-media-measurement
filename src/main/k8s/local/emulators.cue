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

_secret_name:            string @tag("secret_name")
_spannerEmulatorVersion: string @tag("spanner_emulator_version")

#ComponentName: "testing"

objectSets: [
	services,
	pods,
	deployments,
]

_localStorageImageConfig: #ImageConfig & {
	repoSuffix: "emulator/local-storage"
}

services: [Name=string]: #Service & {
	metadata: {
		_component: #ComponentName
		name:       Name
	}
}
services: {
	"spanner-emulator": {
		spec: {
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
	"fake-storage-server": #GrpcService
}

pods: [Name=string]: #Pod & {
	metadata: {
		_component: #ComponentName
		name:       Name
	}
}
pods: {
	"spanner-emulator": Pod={
		spec: {
			_containers: "\(Pod.metadata.name)": {
				image: "gcr.io/cloud-spanner-emulator/emulator:\(_spannerEmulatorVersion)"
			}
			// Emulator DB is in memory, so restarting may hide data loss.
			restartPolicy: "Never"
		}
	}
}

deployments: [Name=string]: #Deployment & {
	_name:       Name
	_secretName: _secret_name
	_system:     #ComponentName
}
deployments: {
	"fake-storage-server": #ServerDeployment & {
		_container: {
			image: _localStorageImageConfig.image
			args: [
				"--debug-verbose-grpc-server-logging=false",
				"--port=8443",
				"--require-client-auth=false",
				"--tls-cert-file=/var/run/secrets/files/kingdom_tls.pem",
				"--tls-key-file=/var/run/secrets/files/kingdom_tls.key",
				"--cert-collection-file=/var/run/secrets/files/all_root_certs.pem",
			]
		}
	}
}
