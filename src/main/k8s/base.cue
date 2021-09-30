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

#AppName:    "measurement-system"
#SecretName: "all-test-certs-m64b868cg4"

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
	_type:      *"ClusterIP" | "NodePort"
	apiVersion: "v1"
	kind:       "Service"
	metadata: {
		name: _name
		annotations: system:              _system
		labels: "app.kubernetes.io/name": #AppName
	}
	spec: {
		selector: app: _name + "-app"
		type: _type
		ports: [{
			name:       "port"
			port:       8080
			protocol:   "TCP"
			targetPort: 8080
		}]
	}
}

#Deployment: {
	_name:     string
	_replicas: int | *1
	_image:    string
	_args: [...string]
	_ports:           [{containerPort: 8080}] | *[]
	_restartPolicy:   string | *"Always"
	_imagePullPolicy: string | *"Never"
	_system:          string
	_jvm_flags:       string | *""
	_dependencies: [...string]
	_resourceRequestCpu:    string | *"0.5"
	_resourceLimitCpu:      string | *"2"
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
						secretName: #SecretName
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
	_ports: [{containerPort: 8080}]
	spec: template: spec: containers: [{
		readinessProbe: {
			exec: command: [
				"/app/grpc_health_probe/file/grpc-health-probe",
				"--addr=:8080",
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
	_image:           string | *""
	_imagePullPolicy: string | *"Always"
	_args: [...string]
	_dependencies: [...string]

	apiVersion: "batch/v1"
	kind:       "Job"
	metadata: {
		name: _name + "-job"
		labels: "app.kubernetes.io/name": #AppName
	}
	spec: template: spec: {
		containers: [{
			name:            _name + "-container"
			image:           _image
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
					secretName: #SecretName
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
	_name: string
	_sourceMatchLabels: [...string]
	_destinationMatchLabels: string

	apiVersion: "networking.k8s.io/v1"
	kind:       "NetworkPolicy"
	metadata: {
		name: _name + "-network-policy"
	}
	spec: {
		podSelector: matchLabels: app: _destinationMatchLabels
		policyTypes: ["Ingress"]
		ingress: [{
			from: [ for d in _sourceMatchLabels {
				podSelector: matchLabels: app: d
			}]
		}]
	}
}

// This policy will deny ingress traffic to all unconfigured pods.
default_deny_ingress: [{
	apiVersion: "networking.k8s.io/v1"
	kind:       "NetworkPolicy"
	metadata:
		name: "default-deny-ingress"
	spec: {
		podSelector: {}
		policyTypes: ["Ingress"]
	}
}]
