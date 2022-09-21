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

// Default config for an Open Telemetry Collector
#OpenTelemetryCollectorConfig:
	"""
      receivers:
        otlp:
          protocols:
            grpc:
              endpoint: 0.0.0.0:\(#OpenTelemetryReceiverPort)

      exporters:
        prometheus:
          endpoint: 0.0.0.0:\(#OpenTelemetryPrometheusExporterPort)

      extensions:
        health_check:

      service:
        extensions: [health_check]
        pipelines:
          metrics:
            receivers: [otlp]
            processors: []
            exporters: [prometheus]
      """

#OpenTelemetryCollectorSidecar: Sidecar={
	_name:   string
	_config: string | *#OpenTelemetryCollectorConfig

	sidecars: {
		"collector-sidecar": {
			apiVersion: "opentelemetry.io/v1alpha1"
			kind:       "OpenTelemetryCollector"
			metadata: name: "\(Sidecar._name)-sidecar"
			spec: {
				mode:   "sidecar"
				config: "\(_config)"
			}
		}
	}
}
