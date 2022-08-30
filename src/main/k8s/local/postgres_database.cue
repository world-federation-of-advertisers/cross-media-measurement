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

#ComponentName: "testing"

objectSets: [
	services,
	pods,
]

services: [Name=string]: #Service & {
	metadata: {
		_component: #ComponentName
		name:       Name
	}
}
services: {
	"postgres": {
		spec: {
			ports: [{
				name: "postgresql"
				port: 5432
			}]
		}
	}
}

pods: [Name=string]: #Pod & {
	metadata: {
		_component: #ComponentName
		name:       Name
	}
}
pods: {
	"postgres": {
		spec: _containers: "postgres": {
			_envVars: {
				"POSTGRES_USER": {
					valueFrom: secretKeyRef: {
						name: _dbSecretName
						key:  "username"
					}
				}
				"POSTGRES_PASSWORD": {
					valueFrom: secretKeyRef: {
						name: _dbSecretName
						key:  "password"
					}
				}
			}
			image: "docker.io/postgres:14.4-alpine"
		}
	}
}
