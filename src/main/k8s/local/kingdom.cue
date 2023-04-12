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

// Number of gRPC threads for the internal API server.
#InternalServerGrpcThreads: 7

// Number of gRPC threads for the system API server.
//
// This serves long-lived streaming RPCs from each Herald which will each occupy
// a thread, so this should be greater than the number of Heralds.
#SystemServerGrpcThreads: 5

objectSets: [ for objectSet in kingdom {objectSet}]

kingdom: #Kingdom & {
	_kingdom_secret_name: _secret_name
	_spannerConfig: database: "kingdom"
	_verboseGrpcServerLogging: true
	_verboseGrpcClientLogging: true

	deployments: {
		"gcp-kingdom-data-server": {
			_container: {
				_grpcThreadPoolSize: #InternalServerGrpcThreads
			}
			spec: template: spec: {
				_dependencies: ["spanner-emulator"]
			}
		}
		"system-api-server": {
			_container: {
				_grpcThreadPoolSize: #SystemServerGrpcThreads
			}
		}
	}
}
