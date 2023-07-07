// Copyright 2021 The Cross-Media Measurement Authors
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

#GCloudProject:           "halo-cmm-dev"
#KingdomPublicApiTarget:  "public.kingdom.dev.halo-cmm.org:8443"
#ContainerRegistryPrefix: "gcr.io/" + #GCloudProject
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

#ExchangeDaemonConfig: {
	secretName:         string
	partyType:          "DATA_PROVIDER" | "MODEL_PROVIDER"
	partyName:          string
	cloudStorageBucket: string
	serviceAccountName: string

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

	dataflow: {
		projectId:                         *#GCloudProject | string
		region:                            string
		serviceAccount:                    string
		tempLocation:                      *"gs://\(cloudStorageBucket)/dataflow-temp/" | string
		workerMachineType:                 *"n1-standard-1" | string
		diskSize:                          *"30" | string
		dataflowWorkerLoggingOptionsLevel: *"INFO" | string
		sdkHarnessOptionsLogLevel:         *"INFO" | string
	}

	_partyId: strings.SplitAfter(partyName, "/")[1]

	args: [
		"--id=\(_partyId)",
		"--party-type=\(partyType)",
		"--google-cloud-storage-bucket=\(cloudStorageBucket)",
		"--tls-cert-file=\(clientTls.certFile)",
		"--tls-key-file=\(clientTls.keyFile)",
		"--tink-key-uri=\(tinkKeyUri)",
		"--privateca-ca-name=\(privateCa.name)",
		"--privateca-pool-id=\(privateCa.poolId)",
		"--privateca-ca-location=\(privateCa.location)",
		"--dataflow-project-id=\(dataflow.projectId)",
		"--dataflow-region=\(dataflow.region)",
		"--dataflow-service-account=\(dataflow.serviceAccount)",
		"--dataflow-temp-location=\(dataflow.tempLocation)",
		"--dataflow-worker-machine-type=\(dataflow.workerMachineType)",
		"--dataflow-disk-size=\(dataflow.diskSize)",
		"--dataflow-worker-logging-options-level=\(dataflow.dataflowWorkerLoggingOptionsLevel)",
		"--sdk-harness-options-log-level=\(dataflow.sdkHarnessOptionsLogLevel)",
	]
}
_exchangeDaemonConfig: #ExchangeDaemonConfig

objectSets: [deployments, networkPolicies]

deployments: [Name=_]: #Deployment & {
	_name:      Name
	_component: "workflow-daemon"
	_podSpec: _container: resources: #DefaultResourceConfig.resources

	spec: {
		replicas: #DefaultResourceConfig.replicas
	}
}
deployments: {
	"example-panel-exchange-daemon": {
		_jvmFlags:   "-Xmx3584m" // 4GiB - 512MiB overhead.
		_secretName: _exchangeDaemonConfig.secretName
		_podSpec: {
			serviceAccountName: _exchangeDaemonConfig.serviceAccountName
			nodeSelector: "iam.gke.io/gke-metadata-server-enabled": "true"
		}
		_podSpec: _container: {
			image:           #ContainerRegistryPrefix + "/panel-exchange/gcloud-example-daemon"
			imagePullPolicy: "Always"
			args:            _exchangeDaemonConfig.args + [
						"--cert-collection-file=/var/run/secrets/files/trusted_certs.pem",
						"--blob-size-limit-bytes=1000000000",
						"--storage-signing-algorithm=EC",
						"--task-timeout=24h",
						"--exchange-api-target=" + #KingdomPublicApiTarget,
						"--exchange-api-cert-host=localhost",
						"--debug-verbose-grpc-client-logging=\(#DebugVerboseGrpcLogging)",
						"--google-cloud-storage-project=" + #GCloudProject,
						"--channel-shutdown-timeout=3s",
						"--polling-interval=1m",
						"--preprocessing-max-byte-size=1000000",
						"--preprocessing-file-count=1000",
						"--x509-common-name=SomeCommonName",
						"--x509-organization=SomeOrganization",
						"--x509-dns-name=example.com",
						"--x509-valid-days=365",
						"--privateca-project-id=" + #GCloudProject,
			]
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
