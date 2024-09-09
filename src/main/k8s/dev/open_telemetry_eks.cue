// Copyright 2023 The Cross-Media Measurement Authors
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

objectSets: [ for objectSet in openTelemetry {objectSet}]

#OpenTelemetryCollector: {
	spec: resources: requests: memory: "48Mi"
}

openTelemetry: #OpenTelemetry & {
	collectors: {
		"default": {
			spec: {
				serviceAccount: "adot-collector"
				config: {
					exporters: {
						prometheusremotewrite: {
							endpoint: #AMPIngestEndpoint
							auth:
								authenticator: "sigv4auth"
						}
					}

					extensions: {
						health_check: {}
						sigv4auth: {
							region:  #AMPRegion
							service: "aps"
						}
					}
					service: {
						extensions: ["health_check", "sigv4auth"]
						pipelines:
							metrics:
								exporters: ["prometheusremotewrite"]
					}
				}
			}
		}
	}
	instrumentations: {
		"java-instrumentation": {
			spec: {
				sampler: {
					type: "always_on"
				}
			}
		}
	}
}
