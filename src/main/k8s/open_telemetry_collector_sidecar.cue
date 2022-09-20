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
	_config:      string | *#OpenTelemetryCollectorConfig

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

	services: [Name=_]: #Service & {
		metadata: {
			_component: "\(Sidecar._name)"
			name:       Name
		}
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
