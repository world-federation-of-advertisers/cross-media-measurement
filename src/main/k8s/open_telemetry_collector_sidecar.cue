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

#OpenTelemetryCollectorSidecar: Sidecar={
	_name:        string
	_podLabelApp: string
	_config?:     string

	configMaps: [#OpenTelemetryCollectorConfigMap & {
		_name: "\(Sidecar._name)-otel-col"
		if _config != _|_ {
			data: {
				"config.yaml": "\(_config)"
			}
		}
	}]

	_container: {
		name:            "open-telemetry-collector-sidecar"
		image:           "otel/opentelemetry-collector:0.59.0"
		imagePullPolicy: "Always"
		args: [
			"--config=/etc/\(#AppName)/\(Sidecar._name)-otel-col/config.yaml",
		]

		ports: [{
			name:          "prom-exporter"
			containerPort: #OpenTelemetryPrometheusExporterPort
			protocol:      "TCP"
		}, {
			name:          "otel-receiver"
			containerPort: #OpenTelemetryReceiverPort
			protocol:      "TCP"
		}]
	}

	_configMapMount: #ConfigMapMount & {
		name: "\(Sidecar._name)-otel-col"
	}

	services: [Name=_]: #Service & {
		_name:   Name
		_system: "\(Sidecar._name)"
	}
	services: {
		"\(Sidecar._name)-prom-exp": {
			spec: {
				selector: app: _podLabelApp
				ports: [{
					name:       "prom-exporter"
					port:       #OpenTelemetryPrometheusExporterPort
					protocol:   "TCP"
					targetPort: #OpenTelemetryPrometheusExporterPort
				}]
			}
		}
	}
}
