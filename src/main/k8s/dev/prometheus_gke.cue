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
	clusterPodMonitorings,
	podMonitorings,
]

clusterPodMonitorings: {
	"gcp-prometheus-pod-monitoring": {
		apiVersion: "monitoring.googleapis.com/v1"
		kind:       "ClusterPodMonitoring"
		metadata: {
			name: "prometheus-pod-monitor"
		}
		spec: {
			selector: matchLabels: scrape: "true"
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
