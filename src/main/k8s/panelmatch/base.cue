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

// TODO(@MarcoPremier): Merge this with base.cue in cross-media-measurement repo

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

#AppName: "halo-panel-exchange"

#GrpcServicePort: 8443

#Target: {
	name:   string
	_caps:  strings.Replace(strings.ToUpper(name), "-", "_", -1)
	target: "$(" + _caps + "_SERVICE_HOST):$(" + _caps + "_SERVICE_PORT)"
}

#SecretMount: {
	name:       string
	secretName: string
	mountPath:  string | *"/var/run/secrets/files"
}

#ConfigMapMount: {
	name:          string
	configMapName: string | *name
	mountPath:     string | *"/etc/\(#AppName)/\(name)"
}

#ResourceQuantity: {
	cpu?:    string
	memory?: string
}

#ResourceRequirements: {
	limits?:   #ResourceQuantity
	requests?: #ResourceQuantity
}

#Container: {
	_secretMounts: [...#SecretMount]
	_configMapMounts: [...#ConfigMapMount]

	image:           string
	imagePullPolicy: "IfNotPresent" | "Never" | "Always"
	args: [...string]
	volumeMounts: [ for mount in _configMapMounts + _secretMounts {
		name:      mount.name
		mountPath: mount.mountPath
		readOnly:  true
	}]
	resources?: #ResourceRequirements
	readinessProbe?: {
		exec: command: [...string]
		periodSeconds: uint32
	}
	...
}

#PodSpec: PodSpec={
	_secretMounts: [...#SecretMount]
	_configMapMounts: [...#ConfigMapMount]
	_container: #Container & {
		_secretMounts:    PodSpec._secretMounts
		_configMapMounts: PodSpec._configMapMounts
	}

	restartPolicy?: "Always" | "Never" | "OnFailure"
	containers: [_container]
	volumes: [ for secretVolume in _secretMounts {
		name: secretVolume.name
		secret: secretName: secretVolume.secretName
	}] + [ for configVolume in _configMapMounts {
		name: configVolume.name
		configMap: name: configVolume.configMapName
	}]
	serviceAccountName?: string
	nodeSelector?: [_=string]: string
	...
}

#CronJob: CronJob={
	_name:        string
	_secretName?: string
	_component:   string
	_jvmFlags:    string | *""
	_configMapMounts: [...#ConfigMapMount]
	_podSpec: #PodSpec & {
		if _secretName != _|_ {
			_secretMounts: [{
				name:       _name + "-files"
				secretName: _secretName
			}]
		}
		_configMapMounts: CronJob._configMapMounts

		restartPolicy: _ | *"Never"
	}

	apiVersion: "batch/v1"
	kind:       "CronJob"
	metadata: {
		name: _name + "-cronjob"
		labels: {
			"app.kubernetes.io/name":      _name
			"app.kubernetes.io/part-of":   #AppName
			"app.kubernetes.io/component": _component
		}
	}
	spec: {
		schedule:          string
		concurrencyPolicy: "Allow" | "Forbid" | "Replace"
		jobTemplate: {
			spec: {
				backoffLimit: uint | *0
				template: {
					metadata: labels: "app.kubernetes.io/name": _name
					spec: _podSpec & {
						containers: [{
							name: _name + "-container"
							env: [{
								name:  "JAVA_TOOL_OPTIONS"
								value: _jvmFlags
							}]
						}]
					}
				}
			}
		}
	}
}

#Deployment: Deployment={
	_name:        string
	_secretName?: string
	_ports:       [{containerPort: #GrpcServicePort}] | *[]
	_component:   string
	_jvmFlags:    string | *""
	_dependencies: [...string]
	_configMapMounts: [...#ConfigMapMount]
	_podSpec: #PodSpec & {
		if _secretName != _|_ {
			_secretMounts: [{
				name:       _name + "-files"
				secretName: _secretName
			}]
		}
		_configMapMounts: Deployment._configMapMounts

		restartPolicy?: string | "Always"
	}

	apiVersion: "apps/v1"
	kind:       "Deployment"
	metadata: {
		name: _name + "-deployment"
		labels: {
			"app.kubernetes.io/name":      _name
			"app.kubernetes.io/part-of":   #AppName
			"app.kubernetes.io/component": _component
		}
	}
	spec: {
		replicas: uint
		selector: matchLabels: "app.kubernetes.io/name": _name
		template: {
			metadata: labels: "app.kubernetes.io/name": _name
			spec: _podSpec & {
				containers: [{
					name:  _name + "-container"
					ports: _ports
					env: [{
						name:  "JAVA_TOOL_OPTIONS"
						value: _jvmFlags
					}]
				}]
				initContainers: [ for ds in _dependencies {
					name:  "init-\(ds)"
					image: "registry.k8s.io/busybox:1.27"
					command: ["sh", "-c", "until nslookup \(ds); do echo waiting for \(ds); sleep 2; done"]
				}]
			}
		}
	}
}

#NetworkPolicyPort: {
	port?:     uint32 | string
	protocol?: "TCP" | "UDP" | "SCTP"
}

#EgressRule: {
	to: [...]
	ports: [...#NetworkPolicyPort]
}

#IngressRule: {
	from: [...]
	ports: [...#NetworkPolicyPort]
}

#NetworkPolicy: {
	_name:    string
	_appName: string
	_sourceAppNames: [...string]
	_destinationAppNames: [...string]
	_ingresses: [Name=_]: #IngressRule
	_egresses: [Name=_]:  #EgressRule

	_ingresses: {
		if len(_sourceAppNames) > 0 {
			pods: {
				from: [ for appName in _sourceAppNames {
					podSelector: matchLabels: "app.kubernetes.io/name": appName
				}]
			}
		}
	}
	_egresses: {
		if len(_destinationAppNames) > 0 {
			pods: {
				to: [ for appName in _destinationAppNames {
					podSelector: matchLabels: "app.kubernetes.io/name": appName
				}]
				ports: [{
					protocol: "TCP"
					port:     #GrpcServicePort
				}]
			}
		}
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

	apiVersion: "networking.k8s.io/v1"
	kind:       "NetworkPolicy"
	metadata: {
		name: _name + "-network-policy"
		labels: {
			"app.kubernetes.io/part-of": #AppName
		}
	}
	spec: {
		podSelector: matchLabels: "app.kubernetes.io/name": _appName
		policyTypes: ["Ingress", "Egress"]
		ingress: [ for _, ingress in _ingresses {ingress}]
		egress: [ for _, egress in _egresses {egress}]
	}
}

// This policy will deny ingress and egress traffic at all unconfigured pods.
default_deny_ingress_and_egress: [{
	apiVersion: "networking.k8s.io/v1"
	kind:       "NetworkPolicy"
	metadata: {
		name: "default-deny-ingress-and-egress"
		labels: {
			"app.kubernetes.io/part-of": #AppName
		}
	}
	spec: {
		podSelector: {}
		policyTypes: ["Ingress", "Egress"]
	}
}]
