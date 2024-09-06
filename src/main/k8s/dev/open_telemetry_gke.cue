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

// Name of K8s service account for OpenTelemetry collector.
#CollectorServiceAccount: "open-telemetry"

objectSets: [ for objectSet in openTelemetry {objectSet}]

#OpenTelemetryCollector: {
	spec: {
		resources: requests: memory: "48Mi"
		podAnnotations: {}
	}
}

openTelemetry: #OpenTelemetry & {
	collectors: {
		"default": {
			spec: {
				serviceAccount: #CollectorServiceAccount
				config: {
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
						resourcedetection: {
							detectors: ["gcp"]
							timeout: "10s"
						}
						transform: {
							// "location", "cluster", "namespace", "job", "instance", and
							// "project_id" are reserved, and metrics containing these labels
							// will be rejected.  Prefix them with exported_ to prevent this.
							metric_statements: [{
								context: "datapoint"
								statements: [
									"set(attributes[\"exported_location\"], attributes[\"location\"])",
									"delete_key(attributes, \"location\")",
									"set(attributes[\"exported_cluster\"], attributes[\"cluster\"])",
									"delete_key(attributes, \"cluster\")",
									"set(attributes[\"exported_namespace\"], attributes[\"namespace\"])",
									"delete_key(attributes, \"namespace\")",
									"set(attributes[\"exported_job\"], attributes[\"job\"])",
									"delete_key(attributes, \"job\")",
									"set(attributes[\"exported_instance\"], attributes[\"instance\"])",
									"delete_key(attributes, \"instance\")",
									"set(attributes[\"exported_project_id\"], attributes[\"project_id\"])",
									"delete_key(attributes, \"project_id\")",
								]
							}]
						}
					}

					exporters: {
						googlecloud: {}
					}

					service: {
						pipelines: {
							metrics: {
								receivers: ["otlp"]
								processors: [
									"batch",
									"memory_limiter",
									"resourcedetection",
									"transform",
								]
								exporters: ["googlecloud"]
							}
							traces: {
								receivers: ["otlp"]
								processors: ["batch", "memory_limiter", "filter"]
								exporters: ["googlecloud"]
							}
						}
					}
				}
			}
		}
	}

	instrumentations: "java-instrumentation": {
		spec: {
			_envVars: {
				OTEL_TRACES_EXPORTER:                                     "otlp"
				OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION: "base2_exponential_bucket_histogram"
			}
		}
	}

	networkPolicies: {
		"opentelemetry-collector": {
			_egresses: {
				// Need to call Google Cloud Monitoring.
				any: {}
			}
		}
	}

	serviceAccounts: {
		"\(#CollectorServiceAccount)": #WorkloadIdentityServiceAccount & {
			_iamServiceAccountName: "open-telemetry"
		}
	}
}
