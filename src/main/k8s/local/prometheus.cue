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

objectSets: [
	clusterRoles,
	serviceAccounts,
	clusterRoleBindings,
	configMaps,
	services,
	pods,
]

clusterRoles: {
	"prometheus-cluster-role": {
		apiVersion: "rbac.authorization.k8s.io/v1"
		kind:       "ClusterRole"
		metadata: {
			name: "prometheus-service-discovery"
		}
		rules: [{
			apiGroups: [""]
			resources: ["services", "pods", "endpoints"]
			verbs: ["get", "list", "watch"]
		}]
	}
}

serviceAccounts: {
	"prometheus-service-account": {
		apiVersion: "v1"
		kind:       "ServiceAccount"
		metadata: {
			name:      "prometheus-service-account"
			namespace: "default"
		}
	}
}

clusterRoleBindings: {
	"prometheus-cluster-role-binding": {
		apiVersion: "rbac.authorization.k8s.io/v1"
		kind:       "ClusterRoleBinding"
		metadata: name: "prometheus-service-discovery-binding"
		roleRef: {
			apiGroup: "rbac.authorization.k8s.io"
			kind:     "ClusterRole"
			name:     "prometheus-service-discovery"
		}
		subjects: [{
			kind:      "ServiceAccount"
			name:      "prometheus-service-account"
			namespace: "default"
		}]
	}
}

configMaps: [#ConfigMap & {
	_name:   "prometheus"
	_system: "prometheus"
	data: {
		"prometheus.yaml": """
			global:
			  scrape_interval: 30s
			  scrape_timeout: 10s
			  evaluation_interval: 30s

			scrape_configs:
			  - job_name: otel-collector
			    honor_labels: true
			    honor_timestamps: true
			    metrics_path: /metrics
			    kubernetes_sd_configs:
			      - role: endpoints
			    relabel_configs:
			      - source_labels: [__meta_kubernetes_pod_container_port_number]
			        action: keep
			        regex: 8889
			      - source_labels: [__meta_kubernetes_namespace]
			        action: drop
			        regex: kube-system
			"""
	}
}]

services: {
	"prometheus": {
		apiVersion: "v1"
		kind:       "Service"
		metadata: {
			name: "prometheus"
			labels: {
				"app.kubernetes.io/part-of":   #AppName
				"app.kubernetes.io/component": "testing"
			}
		}
		spec: {
			selector: app: "prometheus-app"
			ports: [{
				name:     "prometheus"
				port:     9090
				protocol: "TCP"
			}]
		}
	}
}

pods: {
	"prometheus-pod": {
		apiVersion: "v1"
		kind:       "Pod"
		metadata: {
			name: "prometheus-pod"
			labels: {
				app:                           "prometheus-app"
				"app.kubernetes.io/part-of":   #AppName
				"app.kubernetes.io/component": "testing"
			}
		}
		spec: #PodSpec & {
			_mounts: "prometheus": #ConfigMapMount
			_containers: {
				"prometheus": {
					image:           "docker.io/prom/prometheus:v2.38.0"
					imagePullPolicy: "Always"
					args: [
						"--config.file=/etc/\(#AppName)/prometheus/prometheus.yaml",
					]
				}
			}
			restartPolicy:      "Always"
			serviceAccountName: "prometheus-service-account"
		}
	}
}
