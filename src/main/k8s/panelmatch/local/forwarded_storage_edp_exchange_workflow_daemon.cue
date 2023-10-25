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

_exchangeDaemonConfig: {
	secretName: string @tag("secret_name")
	partyName:  string @tag("party_name")
	partyType:  "DATA_PROVIDER"
	clientTls: {
		certFile: "/var/run/secrets/files/edp1_tls.pem"
		keyFile:  "/var/run/secrets/files/edp1_tls.key"
	}
}

deployments: [Name=_]: #Deployment & {
	_name:      Name
	_component: "workflow-daemon"
	_system:    "edp_daemon"
	_podSpec: _container: resources: #DefaultResourceConfig.resources

	spec: {
		replicas: #DefaultResourceConfig.replicas
	}
}
deployments: {
	"data-provider-panel-exchange-daemon": {
		_jvmFlags:   "-Xms512m"
		_secretName: _exchangeDaemonConfig.secretName
		_podSpec: _container: {
			image:           _localStorageImageConfig.image
			imagePullPolicy: "Always"
			args:            _exchangeDaemonConfig.args + [
						"--cert-collection-file=/var/run/secrets/files/edp_trusted_certs.pem",
						"--storage-signing-algorithm=EC",
						"--task-timeout=10m",
						"--exchange-api-target=" + #KingdomPublicApiTarget,
						"--exchange-api-cert-host=localhost",
						"--debug-verbose-grpc-client-logging=true",
						"--channel-shutdown-timeout=3s",
						"--polling-interval=3s",
						"--preprocessing-max-byte-size=1000000",
						"--preprocessing-file-count=1000",
						"--forwarded-storage-service-target=" + (#Target & {name: "dp-private-storage-server"}).target,
						"--forwarded-storage-cert-host=localhost",
			]
		}
	}
}
