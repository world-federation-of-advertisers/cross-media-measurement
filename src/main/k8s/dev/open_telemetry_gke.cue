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

import "encoding/yaml"

#GCloudProject: string @tag("google_cloud_project")

// Name of K8s service account for OpenTelemetry collector.
#CollectorServiceAccount: "open-telemetry"

objectSets: [
	collectors,
	openTelemetry.instrumentations,
	networkPolicies,
	serviceAccounts,
]

serviceAccounts: [Name=string]: #ServiceAccount & {
	metadata: name: Name
}

serviceAccounts: {
	"\(#CollectorServiceAccount)": #WorkloadIdentityServiceAccount & {
		_iamServiceAccountName: "open-telemetry"
	}
}

#OpenTelemetryCollector: {
	spec: resources: requests: memory: "48Mi"
}

openTelemetry: #OpenTelemetry

collectors: openTelemetry.collectors & {
	"default": {
		spec: {
			serviceAccount: #CollectorServiceAccount
			_config: {
				processors: {
					filter: {
						spans: {
							exclude: {
								match_type: "strict"
								attributes: [{
									key:   "rpc.method"
									value: "Check"
								}]
							}
						}
					}
				}

				exporters: {...} | *{
					googlecloud: {
						project: "\(#GCloudProject)"
						trace: {}
					}
				}

				service: {
					pipelines: {
						traces: {
							receivers: ["otlp"]
							processors: ["batch", "filter"]
							exporters: [...] | *["googlecloud"]
						}
					}
				}
			}

			config: yaml.Marshal(_config)
		}
	}
}

networkPolicies: [Name=_]: #NetworkPolicy & {
	_policyPodSelectorMatchLabels: "app.kubernetes.io/component": "opentelemetry-collector"
	_name: Name
}

networkPolicies: {
	"opentelemetry-collector": {
		_ingresses: {
			any: {}
		}
		_egresses: {
			any: {}
		}
	}
}
