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

#NetworkPolicy: {
	_egresses: {
		gkeMetadataServer: {
			to: [{ipBlock: cidr: "127.0.0.1/32"}]
			ports: [{
				protocol: "TCP"
				port:     988
			}]
		}
		openTelemetryCollector: {
			to: [{podSelector: matchLabels: app: "opentelemetry-collector-app"}]
			ports: [{
				port: #OpenTelemetryReceiverPort
			}]
		}
	}

	_ingresses: {
		gmpManagedCollector: {
			from: [{
				namespaceSelector: matchLabels: "kubernetes.io/metadata.name": "gmp-system"
				podSelector: matchLabels: app:                                 "managed-prometheus-collector"
			}]
			ports: [{
				protocol: "TCP"
				port:     #OpenTelemetryPrometheusExporterPort
			}]
		}
	}
}

#ServiceAccountNodeSelector: {
	"iam.gke.io/gke-metadata-server-enabled": "true"
}

#ServiceAccountPodSpec: {
	#PodSpec

	serviceAccountName: string
	nodeSelector:       #ServiceAccountNodeSelector
}

#SpotVmPodSpec: {
	#PodSpec

	_tolerations: "cloud.google.com/gke-spot": {
		operator: "Equal"
		value:    "true"
		effect:   "NoSchedule"
	}
}

#JavaOptions: {
	initialHeapSize: _ | *"32M"
	maxHeapSize:     _ | *"96M"
}
