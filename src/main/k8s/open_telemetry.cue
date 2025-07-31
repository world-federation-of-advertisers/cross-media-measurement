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

// K8s custom resource defined by OpenTelemetry Operator used for creating
// an OpenTelemetry Collector.
#OpenTelemetryCollector: {
	apiVersion: "opentelemetry.io/v1beta1"
	kind:       "OpenTelemetryCollector"
	metadata:   #ObjectMeta & {
		_component: "metrics"
		labels: "app": "opentelemetry-collector-app"
	}
	spec: {
		mode:             *"deployment" | "sidecar"
		image:            string | *"ghcr.io/open-telemetry/opentelemetry-collector-releases/opentelemetry-collector-contrib:0.129.1"
		imagePullPolicy?: "IfNotPresent" | "Always" | "Never"
		config: {
			exporters: {...}
			receivers: {...}
			service: {
				pipelines: [string]: {
					exporters: [...string]
					processors: [...string]
					receivers: [...string]
				}
				extensions?: [...]
				telemetry?: {...}
			}
			connectors?: {...}
			extensions?: {...}
			processors?: {...}
		}
		nodeSelector?: {...}
		podSelector?: {...}
		serviceAccount?: string
		resources?:      #ResourceRequirements
		podAnnotations:  {...} | *{
			"prometheus.io/port":   string | *"\(#OpenTelemetryPrometheusExporterPort)"
			"prometheus.io/scrape": string | *"true"
		}
	}
}

#OpenTelemetry: {
	collectors: [Name=string]: #OpenTelemetryCollector & {
		metadata: name: Name
	}
	collectors: {
		"default": {
			spec: {
				config: {
					receivers: {
						otlp:
							protocols:
								grpc:
									endpoint: "0.0.0.0:\(#OpenTelemetryReceiverPort)"
					}

					processors: {
						// Batch metrics before sending to reduce API usage.
						batch: {
							send_batch_max_size: 200
							send_batch_size:     200
							timeout:             "5s"
						}

						// Drop metrics if memory usage gets too high.
						memory_limiter: {
							check_interval:         "1s"
							limit_percentage:       65
							spike_limit_percentage: 20
						}
					}

					exporters: _ | *{
						prometheus: {
							send_timestamps: true
							endpoint:        "0.0.0.0:\(#OpenTelemetryPrometheusExporterPort)"
							resource_to_telemetry_conversion:
								enabled: true
						}
					}

					extensions: _ | *{
						health_check: {}
					}

					service: {
						extensions: [...] | *["health_check"]
						pipelines: {
							metrics: {
								receivers:  _ | *["otlp"]
								processors: _ | *["batch", "memory_limiter"]
								exporters:  [...] | *["prometheus"]
							}
						}
					}
				}
			}
		}
	}

	instrumentations: {
		"java-instrumentation": {
			apiVersion: "opentelemetry.io/v1alpha1"
			kind:       "Instrumentation"
			metadata: name: "open-telemetry-java-agent"
			spec: {
				_envVars: [string]: string
				_envVars: {
					OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT: "256"
					OTEL_TRACES_EXPORTER:                   _ | *"none"
					OTEL_EXPORTER_OTLP_TRACES_PROTOCOL:     "grpc"
					OTEL_EXPORTER_OTLP_ENDPOINT:            "http://default-collector-headless.default.svc:\(#OpenTelemetryReceiverPort)"
					OTEL_EXPORTER_OTLP_TIMEOUT:             "20000"
					OTEL_EXPORTER_OTLP_METRICS_PROTOCOL:    "grpc"
					OTEL_METRICS_EXPORTER:                  "otlp"
					OTEL_METRIC_EXPORT_INTERVAL:            "30000"
					OTEL_LOGS_EXPORTER:                     "none"
				}
				env: [ for k, v in _envVars {
					name:  k
					value: v
				}]
				java: image: "ghcr.io/open-telemetry/opentelemetry-operator/autoinstrumentation-java:2.18.1"
			}
		}
	}

	networkPolicies: [Name=_]: #NetworkPolicy & {
		_name: Name
	}
	networkPolicies: {
		"to-opentelemetry-collector": {
			_egresses: {
				collector: {
					to: [{
						podSelector: {
							matchLabels: {
								"app.kubernetes.io/component": "opentelemetry-collector"
							}
						}
					}]
				}
			}
			spec: {
				podSelector: {}
				policyTypes: ["Egress"]
			}
		}
		"opentelemetry-collector": {
			_ingresses: {
				allPods: {
					from: [{
						podSelector: {}
					}]
				}
			}
			spec: {
				podSelector: {
					matchLabels: {
						"app.kubernetes.io/component": "opentelemetry-collector"
					}
				}
			}
		}
	}

	serviceAccounts: [Name=string]: #ServiceAccount & {
		metadata: name: Name
	}
}
