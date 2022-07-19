// Copyright 2022 The Cross-Media Measurement Authors
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
  deployments,
]

services: {
	"postgres": {
		apiVersion: "v1"
		kind:       "Service"
		metadata: {
			name: "postgres"
			labels: {
				"app.kubernetes.io/part-of":   #AppName
				"app.kubernetes.io/component": "testing"
			}
		}
		spec: {
			selector: app: "postgres-app"
			ports: [{
				name:       "grpc"
				port:       9030
				protocol:   "TCP"
				targetPort: 9030
			}, {
				name:       "http"
				port:       9040
				protocol:   "TCP"
				targetPort: 9040
			}]
		}
	}
}

deployments: {
	"postgres-deployment": #Deployment & {
		_name:       "postgres"
		_secretName: _secret_name
		_system:     "testing"
		_replicas:              1
		_resourceRequestCpu:    "100m"
		_resourceLimitCpu:      "200m"
		_resourceRequestMemory: "128Mi"
		_resourceLimitMemory:   "256Mi"
		containers: [{
      name:  "postgres"
      image: "postgres:12.11"
      env: [{
        name: "POSTGRES_PASSWORD"
        value: "password"
      }, {
        name: "POSTGRES_USER"
        value: "user"
      }, {
        name: "POSTGRES_DB"
        value: "reporting"
      }]
    }]
	}
}
