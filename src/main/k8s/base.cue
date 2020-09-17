package k8s

import (
	"strings"
)

duchy_pod: [Name=_]: #Pod & {
	_name:   strings.TrimSuffix(Name, "-pod")
	_system: "duchy"
}
kingdom_pod: [Name=_]: #Pod & {
	_name:   strings.TrimSuffix(Name, "-pod")
	_system: "kingdom"
}
duchy_service: [Name=_]: #GrpcService & {
	_name:   Name
	_system: "duchy"
}
kingdom_service: [Name=_]: #GrpcService & {
	_name:   Name
	_system: "kingdom"
}

fake_pod: [Name=_]: {}
fake_service: [Name=_]: {}
kingdom_job: [Name=_]: {}
setup_job: [Name=_]: {}

#Port: {
	name:       string
	port:       uint16
	protocol:   "TCP" | "UDP"
	targetPort: uint16
}

#GrpcService: {
	_name:      string
	_system:    string
	apiVersion: "v1"
	kind:       "Service"
	metadata: {
		name: _name
		annotations: system: _system
	}
	spec: {
		selector: app: _name + "-app"
		type: "ClusterIP"
		ports: [{
			name:       "port"
			port:       8080
			protocol:   "TCP"
			targetPort: 8080
		}]
	}
}

#Pod: {
	_name:  string
	_image: string
	_args: [...string]
	_ports:         [{containerPort: 8080}] | *[]
	_restartPolicy: string | *"Always"
	_system:        string
	apiVersion:     "v1"
	kind:           "Pod"
	metadata: {
		name: _name + "-pod"
		labels: app:         _name + "-app"
		annotations: system: _system
	}
	spec: {
		containers: [{
			name:            _name + "-container"
			image:           _image
			imagePullPolicy: "Never"
			args:            _args
			ports:           _ports
		}]
		restartPolicy: _restartPolicy
	}
}

#ServerPod: #Pod & {
	_ports: [{containerPort: 8080}]
}