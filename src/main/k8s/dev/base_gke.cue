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

defaultNetworkPolicies: {
	"gke": {
		_egresses: {
			// See https://cloud.google.com/kubernetes-engine/docs/how-to/network-policy#network-policy-and-workload-identity
			gkeMetadataServer: {
				to: [{ipBlock: cidr: "169.254.169.252/32"}]
				ports: [{
					protocol: "TCP"
					port:     988
				}]
			}
			gkeDataplaneV2: {
				to: [{ipBlock: cidr: "169.254.169.254/32"}]
				ports: [{
					protocol: "TCP"
					port:     80
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

#ServerDeployment: {
	_container: {
		resources: Resources={
			requests: {
				memory: _ | *"320Mi"
			}
			limits: {
				memory: _ | *Resources.requests.memory
			}
		}
	}
}

#JavaOptions: {
	initialHeapSize:   _ | *maxHeapSize
	maxHeapSize:       _ | *"64M"
	loggingConfigFile: "/etc/java/logging.properties"
}

#JavaConfigMap: #ConfigMap & {
	metadata: name: "java"
	data: "logging.properties":
		"""
			.level = INFO
			
			io.grpc.netty.level=INFO
			sun.net.level=INFO
			
			handlers = java.util.logging.ConsoleHandler
			java.util.logging.ConsoleHandler.level = INFO
			java.util.logging.ConsoleHandler.formatter = org.wfanet.measurement.gcloud.logging.StructuredLoggingFormatter
			"""
}

#PodSpec: {
	_mounts: "java-config": {
		volume: configMap: name: "\(#JavaConfigMap.metadata.name)"
		volumeMount: mountPath: "/etc/java"
	}
}

#ExternalService: {
	// Resource name of the external IP address.
	_ipAddressName?: string

	metadata: annotations: {
		"cloud.google.com/l4-rbs": "enabled"
		if _ipAddressName != _|_ {
			"networking.gke.io/load-balancer-ip-addresses": _ipAddressName
		}
	}
}
