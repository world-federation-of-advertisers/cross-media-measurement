// Copyright 2023 The Cross-Media Measurement Authors
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

import "strings"

#KingdomPublicApiTarget:  "public.kingdom.dev.halo-cmm.org:8443"
#ContainerRegistryPrefix: "localhost:5000/halo"
#DefaultResourceConfig: {
	replicas:  1
	resources: #ResourceRequirements & {
		requests: {
			cpu:    "100m"
			memory: "1Gi"
		}
		limits: {
			cpu:    "400m"
			memory: "4Gi"
		}
	}
}
#DebugVerboseGrpcLogging: true

#ContainerRegistryConfig: {
	repoPrefix: "halo"
	registry:   "localhost:5000"
}

#ExchangeDaemonConfig: {
	secretName:         string
	partyType:          "DATA_PROVIDER" | "MODEL_PROVIDER"
	partyName:          string
	cloudStorageBucket: string
	serviceAccountName?: string

	clientTls: {
		certFile: string
		keyFile:  string
	}

	tinkKeyUri: string

	privateCa: {
		name:     string
		poolId:   string
		location: string
	}

	_partyId: strings.SplitAfter(partyName, "/")[1]

	args: [
		"--id=\(_partyId)",
		"--party-type=\(partyType)",
		"--tls-cert-file=\(clientTls.certFile)",
        "--tls-key-file=\(clientTls.keyFile)",
		"--tink-key-uri=/var/run/secrets/files/edp1_tls.key",
	]
}
_exchangeDaemonConfig: #ExchangeDaemonConfig

objectSets: [deployments, networkPolicies]

deployments: [Name=_]: #Deployment & {
	_name:      Name
	_component: "workflow-daemon"
	_podSpec: _container: resources: #DefaultResourceConfig.resources
	_podSpec: _container: _storageMounts: [{
        name:       "example-panel-exchange-daemon-storage"
    }]
    _podSpec: _storageMounts: [{
        name:       "example-panel-exchange-daemon-storage"
    }]

	spec: {
		replicas: #DefaultResourceConfig.replicas
	}
}
deployments: {
	"example-panel-exchange-daemon": {
		_jvmFlags:   "-Xmx3584m" // 4GiB - 512MiB overhead.
		_secretName: _exchangeDaemonConfig.secretName
		_podSpec: _container: {
            image:           #ContainerRegistryPrefix + "/panel-exchange/file-system-example-daemon"
			imagePullPolicy: "Always"
			args:            _exchangeDaemonConfig.args + [
						"--cert-collection-file=/var/run/secrets/files/trusted_certs.pem",
						"--blob-size-limit-bytes=1000000000",
						"--storage-signing-algorithm=EC",
						"--task-timeout=24h",
						"--exchange-api-target=" + #KingdomPublicApiTarget,
						"--exchange-api-cert-host=localhost",
						"--debug-verbose-grpc-client-logging=\(#DebugVerboseGrpcLogging)",
						"--channel-shutdown-timeout=3s",
						"--polling-interval=1m",
						"--tink-key-uri=some-fake-id",
						"--preprocessing-max-byte-size=1000000",
						"--preprocessing-file-count=1000",
						"--private-storage-root=/var/run/storage"
			]
		}
	}
}

#NetworkPolicy: {
	_egresses: {
		gkeMetadataServer: {
			to: [{ipBlock: cidr: "127.0.0.1/32"}]
			ports: [{
				protocol: "TCP"
				port:     988
			}]
		}
	}
}

networkPolicies: [Name=_]: #NetworkPolicy & {
	_name:    Name
	_appName: Name
}
networkPolicies: {
	"example-panel-exchange-daemon": {
		_ingresses: {
			// No ingress.
		}
		_egresses: {
			// Need to be able to send traffic to storage and Kingdom.
			any: {}
		}
	}
}
