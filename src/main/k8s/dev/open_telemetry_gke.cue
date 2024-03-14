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
#CollectorServiceAccount: "internal-reporting-server"

objectSets: [
	collectors,
	openTelemetry.instrumentations,
	networkPolicies,
]

#OpenTelemetryCollector: {
	spec: resources: requests: memory: "48Mi"
}

openTelemetry: #OpenTelemetry

collectors: [Name=string]: #OpenTelemetryCollector & {
	metadata: name: Name
}

collectors: {
	"default": {
		spec: {
			nodeSelector:   #ServiceAccountNodeSelector
			serviceAccount: #CollectorServiceAccount
			config:         """
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:\(#OpenTelemetryReceiverPort)

processors:
  batch:
    send_batch_size: 200
    timeout: 10s
  filter:
    spans:
      exclude:
        match_type: strict
        attributes:
          - key: rpc.method
            value: Check

exporters:
  prometheus:
    send_timestamps: true
    endpoint: 0.0.0.0:\(#OpenTelemetryPrometheusExporterPort)
    resource_to_telemetry_conversion:
      enabled: true
  googlecloud:
    project: \(#GCloudProject)
    trace:

extensions:
  health_check:

service:
  extensions: [health_check]
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch, filter]
      exporters: [googlecloud]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [prometheus]
"""
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

