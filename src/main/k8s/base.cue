// Copyright 2020 The Cross-Media Measurement Authors
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

// This file includes partial CUE definitions for some object types from the
// Kubernetes API, with some customization for our use cases.
//
// TODO(@SanjayVas): Extract the actual definitions from K8s Go packages. See
// https://cuelang.org/docs/integrations/k8s/#importing-definitions

package k8s

import (
	"strings"
)

listObject: {
	apiVersion: "v1"
	kind:       "List"
	items:      objects
}

objects: [ for objectSet in objectSets for object in objectSet {object}]

#AppName: "halo-cmms"

#PortNumber: int32 & >0 & <65536
#IpProtocol: "UDP" | "TCP" | "SCTP"

// K8s ContainerPort.
#ContainerPort: {
	containerPort: #PortNumber
	name?:         string
	protocol?:     #IpProtocol
}

// K8s ServicePort.
#ServicePort: {
	port:         #PortNumber
	targetPort?:  #PortNumber | string
	protocol?:    #IpProtocol
	name?:        string
	appProtocol?: string
}

#GrpcPort:          8443
#GrpcContainerPort: #ContainerPort & {
	containerPort: #GrpcPort
}
#GrpcServicePort: #ServicePort & {
	name: "grpc-port"
	port: #GrpcPort
}

#HealthPort: 8080

#OpenTelemetryReceiverPort:           4317
#OpenTelemetryPrometheusExporterPort: 8889

// K8s ServiceAccount.
#ServiceAccount: {
	apiVersion: "v1"
	kind:       "ServiceAccount"
	metadata:   #ObjectMeta
}

// K8s Role.
#Role: {
	apiVersion: "rbac.authorization.k8s.io/v1"
	kind:       "Role"
	metadata:   #ObjectMeta
	rules: [...{
		apiGroups?: [...string]
		resources?: [...string]
		verbs: [...string]
		resourceNames?: [...string]
	}]
}

// K8s RoleBinding.
#RoleBinding: {
	apiVersion: "rbac.authorization.k8s.io/v1"
	kind:       "RoleBinding"
	metadata:   #ObjectMeta
	roleRef: {
		apiGroup: string
		kind:     string
		name:     string
	}
	subjects: [...{
		kind:       string
		name:       string
		apiGroup?:  string
		namespace?: string
	}]
}

#ResourceQuantity: {
	cpu?:    string
	memory?: string
}

// K8s ResourceRequirements.
#ResourceRequirements: {
	limits?:   #ResourceQuantity
	requests?: #ResourceQuantity
}

#JavaOptions: {
	maxRamPercentage?:        float
	initialRamPercentage?:    float
	maxHeapSize?:             string
	initialHeapSize?:         string
	profiledCodeHeapSize?:    string
	nonProfiledCodeHeapSize?: string
	maxDirectMemorySize?:     string
	maxCachedBufferSize:      uint | *262144 // 256KiB
	nettyMaxDirectMemory?:    int
	loggingConfigFile?:       string
	heapDumpOnOutOfMemory:    bool | *false
	heapDumpPath?:            string
	exitOnOutOfMemory:        bool | *heapDumpOnOutOfMemory

	_maxRamPercentageOpts: [...string]
	if maxRamPercentage != _|_ {
		_maxRamPercentageOpts: [
			"-XX:MaxRAMPercentage=\(maxRamPercentage)",
			"-XX:MinRAMPercentage=\(maxRamPercentage)",
		]
	}

	options: [...string]
	options: [
		for item in _maxRamPercentageOpts {item},
		if initialRamPercentage != _|_ {
			"-XX:InitialRAMPercentage=\(initialRamPercentage)"
		},
		if maxHeapSize != _|_ {
			"-Xmx\(maxHeapSize)"
		},
		if initialHeapSize != _|_ {
			"-Xms\(initialHeapSize)"
		},
		if profiledCodeHeapSize != _|_ {
			"-XX:ProfiledCodeHeapSize=\(profiledCodeHeapSize)"
		},
		if nonProfiledCodeHeapSize != _|_ {
			"-XX:NonProfiledCodeHeapSize=\(nonProfiledCodeHeapSize)"
		},
		if maxDirectMemorySize != _|_ {
			"-XX:MaxDirectMemorySize=\(maxDirectMemorySize)"
		},
		if nettyMaxDirectMemory != _|_ {
			"-Dio.netty.maxDirectMemory=\(nettyMaxDirectMemory)"
		},
		"-Djdk.nio.maxCachedBufferSize=\(maxCachedBufferSize)",
		if loggingConfigFile != _|_ {
			"-Djava.util.logging.config.file=\(loggingConfigFile)"
		},
		if heapDumpOnOutOfMemory {
			"-XX:+HeapDumpOnOutOfMemoryError"
		},
		if heapDumpPath != _|_ {
			"-XX:HeapDumpPath=\(heapDumpPath)"
		},
		if exitOnOutOfMemory {
			"-XX:+ExitOnOutOfMemoryError"
		},
	]
}

#CommonTarget: {
	host:   string
	port:   uint32 | string
	target: string
}

#HostPortTarget: this={
	#CommonTarget

	target: "\(this.host):\(this.port)"
}

#StringTarget: this={
	#CommonTarget

	let parts = strings.Split(this.target, ":")
	host: parts[0]
	if len(parts) > 1 {
		port: parts[1]
	}
}

#ServiceTarget: {
	#HostPortTarget

	serviceName: string

	let ServiceNameVar = strings.Replace(strings.ToUpper(serviceName), "-", "_", -1)
	host: "$(" + ServiceNameVar + "_SERVICE_HOST)"
	port: "$(" + ServiceNameVar + "_SERVICE_PORT)"
}

#Target: #HostPortTarget | #StringTarget | #ServiceTarget | {
	#ServiceTarget

	name:        string
	serviceName: name
}

#GrpcTarget: GrpcTarget={
	#ServiceTarget | #StringTarget | #HostPortTarget

	port: _ | *443

	certificateHost?: string

	targetOption:          string
	certificateHostOption: string

	args: [
		"\(targetOption)=\(GrpcTarget.target)",
		if (certificateHost != _|_) {"\(certificateHostOption)=\(certificateHost)"},
	]
}

// K8s ConfigMap.
#ConfigMap: {
	apiVersion: "v1"
	kind:       "ConfigMap"
	metadata:   #ObjectMeta

	data: [string]: string
	immutable?: bool
}

// K8s KeyToPath
#KeyToPath: {
	key:  string
	path: string
}

// K8s Volume.
#Volume: {
	name: string
}
#Volume: {
	configMap?: {
		name: string
		items?: [...#KeyToPath]
	}
} | {
	secret?: {
		secretName: string
	}
} | {
	emptyDir?: {
		medium?:    "" | "Memory"
		sizeLimit?: string
	}
}

// K8s VolumeMount.
#VolumeMount: {
	name:      string
	mountPath: string
	readOnly?: bool
	subPath?:  string
}

// Configuration for a Volume and a corresponding VolumeMount.
#Mount: {
	name: string

	let Name = name
	volume: #Volume & {
		name: Name
	}
	volumeMount: #VolumeMount & {
		name:      Name
		mountPath: string
	}
}
#Mount: {
	let Name = volumeMount.name
	volume: configMap: name: string
	volumeMount: {
		mountPath: _ | *"/etc/\(#AppName)/\(Name)"
		readOnly:  true
	}
} | {
	volume: secret: secretName: string
	volumeMount: {
		mountPath: _ | *"/var/run/secrets/files"
		readOnly:  true
	}
} | {
	let Name = volumeMount.name
	volume: emptyDir: {}
	volumeMount: mountPath: _ | *"/run/\(Name)"
}
#ConfigMapMount: Mount=#Mount & {
	volume: configMap: name: _ | *Mount.name
}
#SecretMount: Mount=#Mount & {
	volume: secret: secretName: _ | *Mount.name
}

// K8s ObjectMeta.
#ObjectMeta: {
	_component?: string

	name?: string
	labels: [_=string]:      string
	annotations: [_=string]: string

	labels: {
		"app.kubernetes.io/part-of": #AppName
		if (name != _|_) {
			"app.kubernetes.io/name": name
		}
		if (_component != _|_) {
			"app.kubernetes.io/component": _component
		}
	}
}

// K8s LabelSelectorRequirement.
#LabelSelectorRequirement: {
	key:      string
	operator: "In" | "NotIn" | "Exists" | "DoesNotExist"
	values?: [...string]
}

// K8s LabelSelector.
#LabelSelector: {
	matchExpressions?: [...#LabelSelectorRequirement]
	matchLabels?: [string]: string
}

// K8s ConfigMap.
#ConfigMap: {
	apiVersion: "v1"
	kind:       "ConfigMap"
	metadata:   #ObjectMeta
	data: {...}
}

// K8s Service.
#Service: {
	apiVersion: "v1"
	kind:       "Service"
	metadata:   Metadata=#ObjectMeta & {
		annotations: "system": Metadata._component
	}
	spec: {
		selector: app: "\(metadata.name)-app"
		ports: [...#ServicePort]
		type?:           "ClusterIP" | "LoadBalancer"
		loadBalancerIP?: string | null
		loadBalancerSourceRanges?: [...string]
	}
}

// K8s Service with external load balancer.
#ExternalService: #Service & {
	spec: type: "LoadBalancer"
}

#GrpcService: #Service & {
	metadata: {
		annotations: {
			"cloud.google.com/app-protocols":   "{\"\(#GrpcServicePort.name)\": \"HTTP2\"}"
			"kubernetes.io/ingress.allow-http": "false"
		}
	}
	spec: {
		ports: [#GrpcServicePort]
	}
}

// K8s Toleration.
#Toleration: {
	key:                string
	operator?:          "Equal" | "Exists"
	value?:             string
	effect?:            "NoSchedule" | "PreferNoSchedule" | "NoExecute"
	tolerationSeconds?: int64
}

// K8s PodSpec.
#PodSpec: {
	_mounts: [Name=string]:     #Mount & {name:  Name}
	_volumes: [Name=string]:    #Volume & {name: Name}
	_containers: [Name=string]: #Container & {
		_volumeMounts: {for name, mount in _mounts {"\(name)": mount.volumeMount}}
		name: Name
	}
	_initContainers: [Name=string]: #Container & {
		_volumeMounts: {for name, mount in _mounts {"\(name)": mount.volumeMount}}
		name: Name
	}
	_tolerations: [Key=string]: #Toleration & {
		key: Key
	}
	_dependencies: [...string]

	_volumes: {for name, mount in _mounts {"\(name)": mount.volume}}
	_initContainers: {
		for dep in _dependencies {
			"wait-for-\(dep)": {
				image: "registry.k8s.io/busybox:1.27"
				command: ["sh", "-c",
					"until nslookup \(dep); do echo waiting for \(dep); sleep 2; done",
				]
			}
		}
	}

	restartPolicy?: "Always" | "Never" | "OnFailure"
	containers: [ for _, container in _containers {container}]
	volumes: [ for _, volume in _volumes {volume}]
	serviceAccountName?: string
	nodeSelector?: [_=string]: string
	initContainers: [ for _, initContainer in _initContainers {initContainer}]
	tolerations: [ for _, toleration in _tolerations {toleration}]
}

// K8s PodTemplateSpec.
#PodTemplateSpec: {
	metadata: #ObjectMeta & {
		annotations: {
			"instrumentation.opentelemetry.io/inject-java": string | *"true"
		}
	}
	spec: #PodSpec
}

// K8s PodTemplate.
#PodTemplate: {
	let Name = metadata.name
	let ContainerName = "\(Name)-container"

	_secretName?: string
	_container:   #Container & {
		_javaOptions: {
			heapDumpOnOutOfMemory: true
			heapDumpPath:          "/run/heap-dumps"
		}
	}

	apiVersion: "v1"
	kind:       "PodTemplate"
	metadata:   #ObjectMeta
	template:   #PodTemplateSpec & {
		metadata: {
			labels: {
				app: "\(Name)-app"
			}
			annotations: {
				"instrumentation.opentelemetry.io/container-names": string | *"\(ContainerName)"
			}
		}
		spec: {
			_mounts: {
				if _secretName != _|_ {
					"\(Name)-files": {
						volume: secret: secretName: _secretName
					}
				}
				"heap-dumps": volume: emptyDir: {}
			}
			_containers: "\(ContainerName)": _container
		}
	}
}

// K8s Pod.
#Pod: {
	apiVersion: "v1"
	kind:       "Pod"
	metadata:   Metadata=#ObjectMeta & {
		labels: {
			"app": "\(Metadata.name)-app"
		}
	}
	spec: #PodSpec
}

// K8s Probe.
#Probe: {
	grpc?: {
		port: uint32
	}
	exec?: {
		command: [...string]
	}
	initialDelaySeconds?: uint32
	periodSeconds?:       uint32
	timeoutSeconds?:      uint32
	failureThreshold?:    uint32
}

#EnvVar: {
	name: string
}

#EnvVar: {
	value: string
} | {
	valueFrom:
		secretKeyRef: {
			name: string
			key:  string
		}
}

#EnvVarMap: [Name=string]: #EnvVar & {
	name: Name
}

// K8s Container.
#Container: {
	_volumeMounts: [Name=string]: #VolumeMount & {name: Name}
	_envVars:     #EnvVarMap
	_javaOptions: #JavaOptions

	_envVars: {
		"JAVA_TOOL_OPTIONS": value: strings.Join(_javaOptions.options, " ")
		"MALLOC_ARENA_MAX": value:  _ | *"2"
	}

	name:   string
	image?: string
	args: [...string]
	ports: [...#ContainerPort]
	imagePullPolicy?: "IfNotPresent" | "Never" | "Always"
	command?: [...string]
	volumeMounts: [ for _, volumeMount in _volumeMounts {volumeMount}]
	resources?:      #ResourceRequirements
	readinessProbe?: #Probe
	startupProbe?:   #Probe
	restartPolicy?:  "Always" // For sidecar containers.
	env: [ for _, envVar in _envVars {envVar}]
}

// K8s Deployment.
#Deployment: {
	_name:        string
	_secretName?: string
	_system:      string
	_container:   #Container & {
		_javaOptions: {
			heapDumpOnOutOfMemory: true
			heapDumpPath:          "/run/heap-dumps"
		}
	}
	let ContainerName = "\(_name)-container"

	apiVersion: "apps/v1"
	kind:       "Deployment"
	metadata:   #ObjectMeta & {
		_component: _system
		name:       _name + "-deployment"
		labels: {
			app: _name + "-app"
		}
		annotations: system: _system
	}
	spec: {
		replicas?: int32
		selector:  #LabelSelector & {
			matchLabels: app: _name + "-app"
		}
		template: #PodTemplateSpec & {
			metadata: {
				labels: {
					app: _name + "-app"
				}
				annotations: {
					"instrumentation.opentelemetry.io/container-names": string | *"\(ContainerName)"
				}
			}
			spec: {
				_mounts: {
					if _secretName != _|_ {
						"\(_name)-files": {
							volume: secret: secretName: _secretName
						}
					}
					"heap-dumps": volume: emptyDir: {}
				}
				_containers: "\(ContainerName)": _container
				restartPolicy: restartPolicy | *"Always"
			}
		}
	}
}

#ServerDeployment: #Deployment & {
	_container: {
		_javaOptions: {
			nettyMaxDirectMemory: _ | *0 // Use cleaner.
		}
		ports: [#GrpcContainerPort]
		readinessProbe: {
			grpc: port: #HealthPort
			failureThreshold: 12
			timeoutSeconds:   2
		}
	}
}

// K8s CronJob
#CronJob: {
	_name:        string
	_secretName?: string
	_container:   #Container

	apiVersion: "batch/v1"
	kind:       "CronJob"
	metadata:   #ObjectMeta & {
		name: _name + "-cronjob"
	}
	spec: {
		schedule:                    string
		concurrencyPolicy?:          "Allow" | "Forbid" | "Replace"
		startingDeadlineSeconds?:    int64
		suspend?:                    bool
		successfulJobsHistoryLimit?: int32 & >0
		failedJobsHistoryLimit?:     int32 & >0

		jobTemplate: {
			spec: {
				backoffLimit: uint | *0
				template:     #PodTemplateSpec & {
					metadata: {
						labels: {
							app: _name + "-app"
						}
					}
					spec: {
						if _secretName != _|_ {
							_mounts: "\(_name)-files": {
								volume: secret: secretName: _secretName
							}
						}
						_containers: "\(_name)-container": _container
						restartPolicy: _ | *"Never"
					}
				}
			}
		}
	}
}

// K8s NetworkPolicyPort.
#NetworkPolicyPort: {
	port?:     #PortNumber
	protocol?: #IpProtocol
}

// K8s NetworkPolicyEgressRule.
#NetworkPolicyEgressRule: {
	to?: [...#NetworkPolicyPeer]
	ports?: [...#NetworkPolicyPort]
}

// K8s NetworkPolicyIngressRule.
#NetworkPolicyIngressRule: {
	from?: [...#NetworkPolicyPeer]
	ports?: [...#NetworkPolicyPort]
}

// K8s NetworkPolicyPeer.
#NetworkPolicyPeer: {
	ipBlock?: {...}
	namespaceSelector?: #LabelSelector
	podSelector?:       #LabelSelector
	ports?: [...#NetworkPolicyPort]
}

// K8s NetworkPolicy.
//
// This allows for selectively enabling traffic between pods. The structure
// allows configuring a NetworkPolicy that selects on a pod name and it will
// allow all traffic from pods matching _sourceMatchLabels to pods matching
// _destinationMatchLabels.
#NetworkPolicy: {
	_name:       string
	_app_label?: string
	_sourceMatchLabels: [...string]
	_destinationMatchLabels: [...string]
	_ingresses: [_]: #NetworkPolicyIngressRule
	_egresses: [_]:  #NetworkPolicyEgressRule

	_ingresses: {
		if len(_sourceMatchLabels) > 0 {
			pods: {
				from: [ for appLabel in _sourceMatchLabels {
					podSelector: matchLabels: app: appLabel
				}]
			}
		}
	}
	_egresses: {
		if len(_destinationMatchLabels) > 0 {
			grpc: {
				to: [ for appLabel in _destinationMatchLabels {
					podSelector: matchLabels: app: appLabel
				}]
				ports: [{
					protocol: "TCP"
					port:     #GrpcPort
				}]
			}
		}
	}

	apiVersion: "networking.k8s.io/v1"
	kind:       "NetworkPolicy"
	metadata:   #ObjectMeta & {
		name: _name + "-network-policy"
	}
	spec: {
		podSelector: #LabelSelector & {
			matchLabels: {
				if _app_label != _|_ {
					app: _app_label
				}
			}
		}
		policyTypes?: ["Ingress"] | ["Egress"] | ["Ingress", "Egress"]
		ingress: [ for _, ingress in _ingresses {ingress}]
		egress: [ for _, egress in _egresses {egress}]
	}
}

defaultNetworkPolicies: [Name=string]: #NetworkPolicy & {
	_name: Name
}
defaultNetworkPolicies: {
	// This policy will deny ingress and egress traffic at all unconfigured pods.
	"default-deny": {
		spec: {
			podSelector: {}
			policyTypes: ["Ingress", "Egress"]
		}
	}
	"kube-dns": {
		_egresses: {
			dns: {
				to: [{
					namespaceSelector: {} // Allow DNS only inside the cluster
					podSelector: matchLabels: "k8s-app": "kube-dns"
				}]
				ports: [{
					protocol: "UDP"
					port:     53
				}, {
					protocol: "TCP"
					port:     53
				}]
			}
		}
		spec: {
			podSelector: {}
			policyTypes: ["Egress"]
		}
	}
}

// A simple fanout Ingress base definition
#Ingress: {
	_name:         string
	_host:         string
	_ingressClass: string
	_services: [...{name: string, port: int, path: string}]
	_pathType:  string
	apiVersion: "networking.k8s.io/v1"
	kind:       "Ingress"
	metadata: {
		name: _name + "-ingress"
		annotations: {
			"kubernetes.io/ingress.class": _ingressClass
		}
		labels: {
			"app.kubernetes.io/part-of": #AppName
		}
	}
	spec: {
		rules: [{
			host: _host
			http: paths: [ for s in _services {
				path:     s.path
				pathType: _pathType
				backend: service: {
					name: s.name
					port: number: s.port
				}
			}]
		}]
	}
}
