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

// K8s service account for access to Cloud Monitoring.
#MonitoringServiceAccount: "gmp-monitoring"

objectSets: [
	clusterPodMonitorings,
	podMonitorings,
	rules,
	networkPolicies,
	services,
	deployments,
]

clusterPodMonitorings: {
	"gcp-prometheus-pod-monitoring": {
		apiVersion: "monitoring.googleapis.com/v1"
		kind:       "ClusterPodMonitoring"
		metadata: name: "prometheus-pod-monitor"
		spec: {
			selector: matchLabels: scrape: "true"
			endpoints: [{
				port:     #OpenTelemetryPrometheusExporterPort
				interval: "30s"
			}]
		}
	}
	"opentelemetry-collector-deployment-monitoring": {
		apiVersion: "monitoring.googleapis.com/v1"
		kind:       "ClusterPodMonitoring"
		metadata: name: "opentelemetry-collector-pod-monitor"
		spec: {
			selector: matchLabels: "app.kubernetes.io/component": "opentelemetry-collector"
			endpoints: [{
				port:     #OpenTelemetryPrometheusExporterPort
				interval: "30s"
			}]
		}
	}
}

podMonitorings: {
	"self-monitoring": {
		apiVersion: "monitoring.googleapis.com/v1"
		kind:       "PodMonitoring"
		metadata: {
			namespace: "gmp-system"
			name:      "collector-pod-monitor"
			labels: {
				"app.kubernetes.io/name":    "collector-monitor"
				"app.kubernetes.io/part-of": "google-cloud-managed-prometheus"
			}
		}
		spec: {
			selector: matchLabels: "app.kubernetes.io/name": "collector"
			endpoints: [{
				port:     "prom-metrics"
				interval: "30s"
			}, {
				port:     "cfg-rel-metrics"
				interval: "30s"
			}]
		}
	}
}

rules: {
	"recording": {
		apiVersion: "monitoring.googleapis.com/v1"
		kind:       "Rules"
		metadata: name: "recording-rules"
		spec: groups: [{
			name:     "rpc"
			interval: "5m"
			rules: [{
				record: "rpc_client_request_rate_per_second"
				expr:   "rate(rpc_client_duration_count[5m])"
			}, {
				record: "rpc_client_request_error_rate_per_second"
				expr:   "sum by (instance, rpc_service, rpc_method) (rpc_client_request_rate_per_second unless rpc_client_request_rate_per_second{rpc_grpc_status_code=\"0\"})"
			}]
		}]
	}
}

services: [Name=_]: #Service & {
	metadata: {
		name:       Name
		_component: "prometheus"
	}
}
services: {
	"prometheus-frontend": {
		spec: {
			ports: [{
				name: "prometheus-frontend"
				port: #PrometheusFrontendPort
			}]
			type: "ClusterIP"
		}
	}
}

deployments: [Name=string]: #Deployment & {
	_name:   Name
	_system: "prometheus"
}

deployments: {
	"prometheus-frontend": {
		_container: {
			image:           "gke.gcr.io/prometheus-engine/frontend:v0.4.3-gke.0"
			imagePullPolicy: "IfNotPresent"
			args: [
				"--web.listen-address=:\(#PrometheusFrontendPort)",
				"--query.project-id=\(#GCloudProject)",
			]
		}
		spec: template: {
			metadata: {
				annotations: {
					"instrumentation.opentelemetry.io/inject-java": "false"
				}
			}
			spec: #ServiceAccountPodSpec & {
				_container: resources: requests: memory: "8Mi"
				serviceAccountName: #MonitoringServiceAccount
			}
		}
	}
}

networkPolicies: [Name=_]: #NetworkPolicy & {
	_name: Name
}
networkPolicies: {
	"prometheus-frontend": {
		_app_label: "prometheus-frontend-app"
		_ingresses: "grafana": {
			from: [{
				podSelector: matchLabels: app: "grafana-app"
			}]
			ports: [{
				port:     #PrometheusFrontendPort
				protocol: "TCP"
			}]
		}
		_egresses: {
			any: {}
		}
	}
}
