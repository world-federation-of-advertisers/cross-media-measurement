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

#AppName: "measurement-system"

#Target: {
	name:   string
	_caps:  strings.Replace(strings.ToUpper(name), "-", "_", -1)
	target: "$(" + _caps + "_SERVICE_HOST):$(" + _caps + "_SERVICE_PORT)"
}

#Port: {
	name:       string
	port:       uint16
	protocol:   "TCP" | "UDP"
	targetPort: uint16
}

#GrpcService: {
	_name:      string
	_system:    string
	_type:      *"ClusterIP" | "LoadBalancer"
	apiVersion: "v1"
	kind:       "Service"
	metadata: {
		name: _name
		annotations: {
			system:                             _system
			"cloud.google.com/app-protocols":   '{"grpc-port":"HTTP2"}'
			"kubernetes.io/ingress.allow-http": "false"
		}
		labels: "app.kubernetes.io/name": #AppName
	}
	spec: {
		selector: app: _name + "-app"
		type: _type
		ports: [{
			name:       "grpc-port"
			port:       8443
			protocol:   "TCP"
			targetPort: 8443
		}]
	}
}

#Deployment: {
	_name:       string
	_replicas:   int | *1
	_secretName: string | *""
	_image:      string
	_args: [...string]
	_ports:           [{containerPort: 8443}] | *[]
	_restartPolicy:   string | *"Always"
	_imagePullPolicy: string | *"Never"
	_system:          string
	_jvm_flags:       string | *""
	_dependencies: [...string]
	_resourceRequestCpu:    string | *"200m"
	_resourceLimitCpu:      string | *"400m"
	_resourceRequestMemory: string | *"256Mi"
	_resourceLimitMemory:   string | *"512Mi"
	apiVersion:             "apps/v1"
	kind:                   "Deployment"
	metadata: {
		name: _name + "-deployment"
		labels: {
			app:                      _name + "-app"
			"app.kubernetes.io/name": #AppName
		}
		annotations: system: _system
	}
	spec: {
		replicas: _replicas
		selector: matchLabels: app: _name + "-app"
		template: {
			metadata: labels: app: _name + "-app"
			spec: {
				containers: [{
					name:  _name + "-container"
					image: _image
					resources: requests: {
						memory: _resourceRequestMemory
						cpu:    _resourceRequestCpu
					}
					resources: limits: {
						memory: _resourceLimitMemory
						cpu:    _resourceLimitCpu
					}
					imagePullPolicy: _imagePullPolicy
					args:            _args
					ports:           _ports
					env: [{
						name:  "JAVA_TOOL_OPTIONS"
						value: _jvm_flags
					}]
					volumeMounts: [{
						name:      _name + "-files"
						mountPath: "/var/run/secrets/files"
						readOnly:  true
					}]
					readinessProbe?: {
						exec: command: [...string]
						periodSeconds: uint32
					}
				}]
				volumes: [{
					name: _name + "-files"
					secret: {
						secretName: _secretName
					}
				}]
				initContainers: [ for ds in _dependencies {
					name:  "init-\(ds)"
					image: "gcr.io/google-containers/busybox:1.27"
					command: ["sh", "-c", "until nslookup \(ds); do echo waiting for \(ds); sleep 2; done"]
				}]
				restartPolicy: _restartPolicy
			}
		}
	}
}

#ServerDeployment: #Deployment & {
	_ports: [{containerPort: 8443}]
	spec: template: spec: containers: [{
		readinessProbe: {
			exec: command: [
				"/app/grpc_health_probe/file/grpc-health-probe",
				"--addr=:8443",
				"--tls=true",
				"--tls-ca-cert=/var/run/secrets/files/all_root_certs.pem",
				"--tls-client-cert=/var/run/secrets/files/aggregator_tls.pem",
				"--tls-client-key=/var/run/secrets/files/aggregator_tls.key",
			]
			periodSeconds: 60
		}}]
}

#Job: {
	_name:            string
	_secretName:      string | *""
	_image:           string | *""
	_imagePullPolicy: string | *"Always"
	_args: [...string]
	_dependencies: [...string]
	_resourceRequestCpu:    string | *"200m"
	_resourceLimitCpu:      string | *"400m"
	_resourceRequestMemory: string | *"256Mi"
	_resourceLimitMemory:   string | *"512Mi"

	apiVersion: "batch/v1"
	kind:       "Job"
	metadata: {
		name: _name + "-job"
		labels: "app.kubernetes.io/name": #AppName
	}
	spec: template: spec: {
		containers: [{
			name:  _name + "-container"
			image: _image
			resources: requests: {
				memory: _resourceRequestMemory
				cpu:    _resourceRequestCpu
			}
			resources: limits: {
				memory: _resourceLimitMemory
				cpu:    _resourceLimitCpu
			}
			imagePullPolicy: _imagePullPolicy
			args:            _args
			volumeMounts: [{
				name:      _name + "-files"
				mountPath: "/var/run/secrets/files"
				readOnly:  true
			}]
		}]
		volumes: [
			{
				name: _name + "-files"
				secret: {
					secretName: _secretName
				}
			},
		]
		restartPolicy: "OnFailure"
	}
}

// NetworkPolicy allows for selectively enabling traffic between pods
// https://kubernetes.io/docs/concepts/services-networking/network-policies/#networkpolicy-resource
//
// This structure allows configuring a NetworkPolicy that selects on a pod name and it
// will allow all traffic from pods matching _sourceMatchLabels to pods matching _destinationMatchLabels
//
#NetworkPolicy: {
	_name:      string
	_app_label: string
	_sourceMatchLabels: [...string]
	_destinationMatchLabels: [...string]

	apiVersion: "networking.k8s.io/v1"
	kind:       "NetworkPolicy"
	metadata: {
		name: _name + "-network-policy"
	}
	spec: {
		podSelector: matchLabels: app: _app_label
		policyTypes: ["Ingress", "Egress"]
		ingress: [{
			from: [ for d in _sourceMatchLabels {
				podSelector: matchLabels: app: d
			}]
			ports: [{
				protocol: "TCP"
				port:     8443
			}]
		}]
		egress: [{
			to: [ for d in _destinationMatchLabels {
				podSelector: matchLabels: app: d
			}]
		}, {
			to: [{
				namespaceSelector: {} // Allow DNS only inside the cluster
				podSelector: matchLabels: "k8s-app": "kube-dns"
			}]
			ports: [{
				protocol: "UDP" // To allow DNS resolution
				port:     53
			}, {
				protocol: "TCP" // To allow DNS resolution
				port:     53
			}]
		}]
	}
}

// This policy will deny ingress and egress traffic at all unconfigured pods.
default_deny_ingress_and_egress: [{
	apiVersion: "networking.k8s.io/v1"
	kind:       "NetworkPolicy"
	metadata:
		name: "default-deny-ingress-and-egress"
	spec: {
		podSelector: {}
		policyTypes: ["Ingress", "Egress"]
	}
}]

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
