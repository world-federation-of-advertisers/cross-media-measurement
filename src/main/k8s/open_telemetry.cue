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
	apiVersion: "opentelemetry.io/v1alpha1"
	kind:       "OpenTelemetryCollector"
	metadata:   #ObjectMeta & {
		_component: "metrics"
		labels: "app": "opentelemetry-collector-app"
	}
	spec: {
		mode:             *"deployment" | "sidecar"
		image:            string | *"docker.io/otel/opentelemetry-collector-contrib:0.62.0"
		imagePullPolicy?: "IfNotPresent" | "Always" | "Never"
		config:           string
		nodeSelector?: {...}
		podSelector?: {...}
		serviceAccount?: string
		resources?:      #ResourceRequirements
		podAnnotations: {
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
				config: """
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:\(#OpenTelemetryReceiverPort)

processors:
  batch:
    send_batch_size: 200
    timeout: 10s

exporters:
  prometheus:
    send_timestamps: true
    endpoint: 0.0.0.0:\(#OpenTelemetryPrometheusExporterPort)
    resource_to_telemetry_conversion:
      enabled: true

extensions:
  health_check:

service:
  extensions: [health_check]
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [prometheus]
"""
			}
		}
	}

	instrumentations: {
		"java-instrumentation": {
			apiVersion: "opentelemetry.io/v1alpha1"
			kind:       "Instrumentation"
			metadata: name: "open-telemetry-java-agent"
			spec: {
				env: [
					{
						name:  "OTEL_TRACES_EXPORTER"
						value: "none"
					}, {
						name:  "OTEL_EXPORTER_OTLP_ENDPOINT"
						value: #OpenTelemetryCollectorEndpoint
					}, {
						name:  "OTEL_EXPORTER_OTLP_TIMEOUT"
						value: "20000"
					}, {
						name:  "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"
						value: "grpc"
					}, {
						name:  "OTEL_METRICS_EXPORTER"
						value: "otlp"
					}, {
						name:  "OTEL_METRIC_EXPORT_INTERVAL"
						value: "30000"
					},
				]
				java: image: "ghcr.io/open-telemetry/opentelemetry-operator/autoinstrumentation-java:1.18.0"
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
		}
	}
}
