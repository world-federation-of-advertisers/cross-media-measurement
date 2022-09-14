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

#OpenTelemetryCollector: {
	_images: [Name=_]: string
	_open_telemetry_collector_secret_name: string

	configMaps: [#OpenTelemetryCollectorConfigMap & {
		_name: "open-telemetry-collector"
	}]

	deployments: [Name=string]: #Deployment & {
		_name:       Name
		_secretName: _open_telemetry_collector_secret_name
		_system:     "open-telemetry"
		_container: {
			image:           _images[_name]
			imagePullPolicy: "Always"
		}
	}
	deployments: {
		"open-telemetry-collector": {
			_container: {
				args: [
					"--config=/etc/\(#AppName)/open-telemetry-collector/config.yaml",
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

			spec: template: spec: {
				_projectionMounts: "open-telemetry-collector": #ConfigMapMount
			}
		}
	}

	services: [Name=_]: #Service & {
		_name:   Name
		_system: "open-telemetry"
	}
	services: {
		"open-telemetry-receiver": {
			spec: {
				selector: app: deployments["open-telemetry-collector"].metadata.labels.app
				ports: [{
					name:       "otel-receiver"
					port:       #OpenTelemetryReceiverPort
					protocol:   "TCP"
					targetPort: #OpenTelemetryReceiverPort
				}]
			}
		}
		"prometheus-exporter": {
			spec: {
				selector: app: deployments["open-telemetry-collector"].metadata.labels.app
				ports: [{
					name:       "prom-exporter"
					port:       #OpenTelemetryPrometheusExporterPort
					protocol:   "TCP"
					targetPort: #OpenTelemetryPrometheusExporterPort
				}]
			}
		}
	}

	networkPolicies: [Name=_]: #NetworkPolicy & {
		_name: Name
	}

	networkPolicies: {
		"open-telemetry-collector": {
			_app_label: "open-telemetry-collector-app"
			_ingresses: {
				grpc: {
					ports: [{
						port: #OpenTelemetryReceiverPort
					}]
				}
				http: {
					ports: [{
						port: #OpenTelemetryPrometheusExporterPort
					}]
				}
			}
		}
	}
}
