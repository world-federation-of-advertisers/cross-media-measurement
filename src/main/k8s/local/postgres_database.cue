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

_dbSecretName: string @tag("db_secret_name")

objectSets: [
  services,
  pods,
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
				name:       "postgresql"
				port:       5432
				protocol:   "TCP"
				targetPort: 5432
			}]
		}
	}
}

pods: {
	"postgres-pod": {
		apiVersion: "v1"
		kind:       "Pod"
		metadata: {
			name: "postgres-pod"
			labels: {
				app:                           "postgres-app"
				"app.kubernetes.io/part-of":   #AppName
				"app.kubernetes.io/component": "testing"
			}
		}
		spec: containers: [{
			name:  "postgres"
			image: "docker.io/postgres:14.4-alpine"
      env: [{
        name: "POSTGRES_USER"
        valueFrom:
          secretKeyRef: {
            name: _dbSecretName
            key:  "username"
          }
      }, {
        name: "POSTGRES_PASSWORD"
        valueFrom:
          secretKeyRef: {
            name: _dbSecretName
            key:  "password"
          }
      }]
		}]
	}
}
