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

#KingdomPublicApiTarget: (#Target & {name: "v2alpha-public-api-server"}).target

#NetworkPolicy: {
	_egresses: {
		// Need to send external traffic.
		any: {}
	}
}

#DefaultResourceConfig: {
	replicas:  1
	resources: #ResourceRequirements & {
		requests: {
			cpu:    "25m"
			memory: "256Mi"
		}
		limits: {
			cpu:    "100m"
			memory: "1Gi"
		}
	}
}

#ExchangeDaemonConfig: {
	secretName: string
	partyType:  "DATA_PROVIDER" | "MODEL_PROVIDER"
	partyName:  string

	clientTls: {
		certFile: string
		keyFile:  string
	}

	privateCa: {
		name:     string
		poolId:   string
		location: string
	}

	args: [
		"--id=\(partyName)",
		"--party-type=\(partyType)",
		"--tls-cert-file=\(clientTls.certFile)",
		"--tls-key-file=\(clientTls.keyFile)",
	]
}
_exchangeDaemonConfig: #ExchangeDaemonConfig

objectSets: [deployments]

_localStorageImageConfig: #ImageConfig & {
	repoSuffix: "panel-exchange/forwarded-storage-daemon"
}
