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
#ContainerRegistryPrefix: "{account_id}.dkr.ecr.{region}.amazonaws.com"
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

	_partyId: strings.SplitAfter(partyName, "/")[1]

	args: [
		"--id=\(_partyId)",
		"--party-type=\(partyType)",
		"--storage-signing-algorithm=RSA",
		"--tink-key-uri=\(tinkKeyUri)",
		"--tls-cert-file=\(clientTls.certFile)",
		"--tls-key-file=\(clientTls.keyFile)",
	]
}

#DefaultAwsConfig: {
	region:          string
	storageBucket:   string
	certArn:         string
	kingdomApi:      string
	commonName:      string
	orgName:         string
	dns:             string
	containerPrefix: string
}

_exchangeDaemonConfig: #ExchangeDaemonConfig
_defaultAwsConfig:     #DefaultAwsConfig

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
			// nodeSelector: "iam.gke.io/gke-metadata-server-enabled": "true"
		}
		_podSpec: _container: {
			image:           _defaultAwsConfig.containerPrefix + "panel-exchange/aws-example-daemon"
			imagePullPolicy: "Always"
			args:            _exchangeDaemonConfig.args + [
						"--cert-collection-file=/var/run/secrets/files/trusted_certs.pem",
						"--blob-size-limit-bytes=1000000000",
						"--storage-signing-algorithm=EC",
						"--task-timeout=24h",
						"--exchange-api-target=" + _defaultAwsConfig.kingdomApi,
						"--exchange-api-cert-host=localhost",
						"--debug-verbose-grpc-client-logging=\(#DebugVerboseGrpcLogging)",
						"--channel-shutdown-timeout=3s",
						"--polling-interval=1m",
						"--preprocessing-max-byte-size=1000000",
						"--preprocessing-file-count=1000",
						"--max-parallel-claimed-exchange-steps=1",
						"--x509-common-name=" + _defaultAwsConfig.commonName,
						"--x509-organization=" + _defaultAwsConfig.orgName,
						"--x509-dns-name=" + _defaultAwsConfig.dns,
						"--x509-valid-days=365",
						"--s3-region=" + _defaultAwsConfig.region,
						"--s3-storage-bucket=" + _defaultAwsConfig.storageBucket,
						"--certificate-authority-arn=" + _defaultAwsConfig.certArn,
						"--certificate-authority-csr-signature-algorithm=ECDSA_WITH_SHA256",
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
