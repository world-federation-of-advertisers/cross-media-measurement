// Copyright 2022 The Cross-Media Measurement Authors
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

#Reporting: Reporting={
	_verboseGrpcServerLogging: bool | *false
	_verboseGrpcClientLogging: bool | *false

	_postgresConfig: #PostgresConfig

	_internalApiTarget: #GrpcTarget & {
		serviceName:           "postgres-reporting-data-server"
		targetOption:          "--internal-api-target"
		certificateHostOption: "--internal-api-cert-host"
	}
	_kingdomApiTarget: #GrpcTarget & {
		targetOption:          "--kingdom-api-target"
		certificateHostOption: "--kingdom-api-cert-host"
	}

	_images: [Name=_]: string
	_imagePullPolicy:    string
	_secretName:         string
	_mcConfigSecretName: string

	_tlsArgs: [
		"--tls-cert-file=/var/run/secrets/files/reporting_tls.pem",
		"--tls-key-file=/var/run/secrets/files/reporting_tls.key",
	]
	_reportingCertCollectionFileFlag:   "--cert-collection-file=/var/run/secrets/files/all_root_certs.pem"
	_akidToPrincipalMapFileFlag:        "--authority-key-identifier-to-principal-map-file=/etc/\(#AppName)/config-files/authority_key_identifier_to_principal_map.textproto"
	_measurementConsumerConfigFileFlag: "--measurement-consumer-config-file=/var/run/secrets/files/config/mc/measurement_consumer_config.textproto"
	_signingPrivateKeyStoreDirFlag:     "--signing-private-key-store-dir=/var/run/secrets/files"
	_encryptionKeyPairDirFlag:          "--key-pair-dir=/var/run/secrets/files"
	_encryptionKeyPairConfigFileFlag:   "--key-pair-config-file=/etc/\(#AppName)/config-files/encryption_key_pair_config.textproto"
	_debugVerboseGrpcClientLoggingFlag: "--debug-verbose-grpc-client-logging=\(_verboseGrpcClientLogging)"
	_debugVerboseGrpcServerLoggingFlag: "--debug-verbose-grpc-server-logging=\(_verboseGrpcServerLogging)"

	services: [Name=_]: #GrpcService & {
		_name:   Name
		_system: "reporting"
	}
	services: {
		"postgres-reporting-data-server": {}
		"v1alpha-public-api-server": _type: "LoadBalancer"
	}

	jobs: [Name=_]: #Job & {
		_name: Name
	}

	deployments: [Name=_]: #ServerDeployment & {
		_name:       Name
		_secretName: Reporting._secretName
		_system:     "reporting"
		_container: {
			image:           _images[_name]
			imagePullPolicy: Reporting._imagePullPolicy
		}
	}
	deployments: {
		"postgres-reporting-data-server": {
			_container: args: [
						_reportingCertCollectionFileFlag,
						_debugVerboseGrpcServerLoggingFlag,
						"--port=8443",
						"--health-port=8080",
			] + _postgresConfig.flags + _tlsArgs

			_updateSchemaContainer: Container=#Container & {
				image:           _images[Container.name]
				args:            _postgresConfig.flags
				imagePullPolicy: _container.imagePullPolicy
			}

			spec: template: spec: _initContainers: {
				"update-reporting-schema": _updateSchemaContainer
			}
		}

		"v1alpha-public-api-server": {
			_container: args: [
						_debugVerboseGrpcClientLoggingFlag,
						_debugVerboseGrpcServerLoggingFlag,
						_reportingCertCollectionFileFlag,
						_akidToPrincipalMapFileFlag,
						_measurementConsumerConfigFileFlag,
						_signingPrivateKeyStoreDirFlag,
						_encryptionKeyPairDirFlag,
						_encryptionKeyPairConfigFileFlag,
						"--port=8443",
						"--health-port=8080",
			] + _tlsArgs + _internalApiTarget.args + _kingdomApiTarget.args

			spec: template: spec: {
				_mounts: {
					"mc-config": {
						volume: secret: secretName: Reporting._mcConfigSecretName
						volumeMount: mountPath: "/var/run/secrets/files/config/mc/"
					}
					"config-files": #ConfigMapMount
				}
				_dependencies: ["postgres-reporting-data-server"]
			}
		}
	}

	networkPolicies: [Name=_]: #NetworkPolicy & {
		_name: Name
	}

	networkPolicies: {
		"internal-reporting-data-server": {
			_app_label: "postgres-reporting-data-server-app"
			_sourceMatchLabels: [
				"v1alpha-public-api-server-app",
			]
			_egresses: {
				// Needs to call out to Postgres server.
				any: {}
			}
		}
		"public-reporting-api-server": {
			_app_label: "v1alpha-public-api-server-app"
			_destinationMatchLabels: ["postgres-reporting-data-server-app"]
			_ingresses: {
				gRpc: {
					ports: [{
						port: #GrpcPort
					}]
				}
			}
			_egresses: {
				// Needs to call out to Kingdom.
				any: {}
			}
		}
	}
}
