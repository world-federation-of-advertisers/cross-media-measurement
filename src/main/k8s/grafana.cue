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

#Grafana: {
	_prometheus_url: string

	configmaps: [Name=_]: #ConfigMap & {
		metadata: {
			_component: "grafana"
			name:       Name
		}
	}
	configmaps: {
		"grafana-datasource-and-dashboard-provider": {
			data: {
				"dashboard_provider.yaml": """
					  apiVersion: 1

					  providers:
					    - name: provider
					      folders: dashboards
					      type: file
					      updateIntervalSeconds: 30
					      allowUiUpdates: true
					      options:
					        path: /etc/grafana/dashboards
					        foldersFromFilesStructure: true
					"""

				"datasource.yaml": """
            apiVersion: 1

            datasources:
              - name: prometheus
                type: prometheus
                access: proxy
                uid: prometheus
                url: \(_prometheus_url)
                basicAuth: false
                isDefault: true
                jsonData:
                  timeInterval: '30s'
                version: 1
                editable: false
          """
			}
		}
	}

	services: [Name=_]: #Service & {
		metadata: {
			name:       Name
			_component: "grafana"
		}
	}
	services: {
		"grafana": {
			spec: {
				ports: [{
					name: "grafana"
					port: 3000
				}]
				type: "ClusterIP"
			}
		}
	}

	deployments: [Name=string]: #Deployment & {
		_name:   Name
		_system: "grafana"
	}
	deployments: {
		"grafana": {
			_container: {
				image:           "docker.io/grafana/grafana-oss:9.2.1"
				imagePullPolicy: "Always"
			}
			spec: template: {
				metadata: {
					labels: {
						scrape: "false"
					}
					annotations: {
						"sidecar.opentelemetry.io/inject":              "false"
						"instrumentation.opentelemetry.io/inject-java": "false"
						"prometheus.io/scrape":                         "false"
					}
				}
				spec: _mounts: {
					"grafana-config": #Mount & {
						volume: secret: secretName: "grafana-config"
						volumeMount: {
							mountPath: "/etc/grafana/grafana.ini"
							subPath:   "grafana.ini"
						}
					}
					"grafana-dashboard-provider": #Mount & {
						volume: configMap: {
							name: "grafana-datasource-and-dashboard-provider"
							items: [{
								key:  "dashboard_provider.yaml"
								path: "dashboard_provider.yaml"
							}]
						}
						volumeMount: mountPath: "/etc/grafana/provisioning/dashboards"
					}
					"grafana-datasource": #Mount & {
						volume: configMap: {
							name: "grafana-datasource-and-dashboard-provider"
							items: [{
								key:  "datasource.yaml"
								path: "datasource.yaml"
							}]
						}
						volumeMount: mountPath: "/etc/grafana/provisioning/datasources"
					}
				}
			}
		}
	}
}
