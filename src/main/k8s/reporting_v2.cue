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

#Reporting: Reporting={
	_verboseGrpcServerLogging: bool | *false
	_verboseGrpcClientLogging: bool | *false

	_reportSchedulingCronSchedule: string | *"30 6 * * *" // Daily at 6:30 AM

	_certificateCacheExpirationDuration:  string | *"60m"
	_dataProviderCacheExpirationDuration: string | *"60m"

	_postgresConfig:         #PostgresConfig
	_reportingSpannerConfig: #SpannerConfig & {
		database: "reporting"
	}
	_accessSpannerConfig: #SpannerConfig & {
		database: "access"
	}

	_internalApiTarget: #GrpcTarget & {
		serviceName:           "postgres-internal-reporting-server"
		certificateHost:       "localhost"
		targetOption:          "--internal-api-target"
		certificateHostOption: "--internal-api-cert-host"
	}
	_accessInternalApiTarget: #GrpcTarget & {
		serviceName:           "access-internal-api-server"
		certificateHost:       "localhost"
		targetOption:          "--access-internal-api-target"
		certificateHostOption: "--access-internal-api-cert-host"
	}
	_accessApiTarget: #GrpcTarget & {
		serviceName:           "access-public-api-server"
		certificateHost:       "localhost"
		targetOption:          "--access-api-target"
		certificateHostOption: "--access-api-cert-host"
	}
	_kingdomApiTarget: #GrpcTarget & {
		targetOption:          "--kingdom-api-target"
		certificateHostOption: "--kingdom-api-cert-host"
	}

	_imageSuffixes: [_=string]: string
	_imageSuffixes: {
		"update-reporting-spanner-schema":     string | *"reporting/v2/spanner-update-schema"
		"update-reporting-postgres-schema":    string | *"reporting/v2/postgres-update-schema"
		"postgres-internal-reporting-server":  string | *"reporting/v2/internal-server"
		"reporting-v2alpha-public-api-server": string | *"reporting/v2/v2alpha-public-api"
		"report-scheduling":                   string | *"reporting/v2/report-scheduling"
		"update-access-schema":                string | *"access/update-schema"
		"access-internal-api-server":          string | *"access/internal-api"
		"access-public-api-server":            string | *"access/public-api"
	}
	_imageConfigs: [_=string]: #ImageConfig
	_imageConfigs: {
		for name, suffix in _imageSuffixes {
			"\(name)": {repoSuffix: suffix}
		}
	}
	_images: {
		for name, config in _imageConfigs {
			"\(name)": config.image
		}
	}
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
	_metricSpecConfigFileFlag:          "--metric-spec-config-file=/etc/\(#AppName)/config-files/metric_spec_config.textproto"
	_knownEventGroupMetadataTypeFlag:   "--known-event-group-metadata-type=/etc/\(#AppName)/config-files/known_event_group_metadata_type_set.pb"
	_debugVerboseGrpcClientLoggingFlag: "--debug-verbose-grpc-client-logging=\(_verboseGrpcClientLogging)"
	_debugVerboseGrpcServerLoggingFlag: "--debug-verbose-grpc-server-logging=\(_verboseGrpcServerLogging)"

	services: [Name=_]: #GrpcService & {
		metadata: {
			_component: "reporting"
			name:       Name
		}
	}
	services: {
		"postgres-internal-reporting-server": {}
		"reporting-v2alpha-public-api-server": #ExternalService
		"access-internal-api-server": {}
		"access-public-api-server": #ExternalService
	}

	deployments: [Name=_]: #ServerDeployment & {
		_name:       Name
		_secretName: Reporting._secretName
		_system:     "reporting"
		_container: {
			image: _images[_name]
		}
	}
	deployments: {
		"postgres-internal-reporting-server": {
			_container: args: [
						_reportingCertCollectionFileFlag,
						_debugVerboseGrpcServerLoggingFlag,
						"--port=8443",
						"--health-port=8080",
			] + _postgresConfig.flags + _reportingSpannerConfig.flags + _tlsArgs

			_updatePostgresSchemaContainer: Container=#Container & {
				image:            _images[Container.name]
				args:             _postgresConfig.flags
				imagePullPolicy?: _container.imagePullPolicy
			}

			_updateSpannerSchemaContainer: Container=#Container & {
				image:            _images[Container.name]
				args:             _reportingSpannerConfig.flags
				imagePullPolicy?: _container.imagePullPolicy
			}

			spec: template: spec: _initContainers: {
				"update-reporting-postgres-schema": _updatePostgresSchemaContainer
				"update-reporting-spanner-schema":  _updateSpannerSchemaContainer
			}
		}

		"reporting-v2alpha-public-api-server": {
			_container: args: [
						_debugVerboseGrpcClientLoggingFlag,
						_debugVerboseGrpcServerLoggingFlag,
						_reportingCertCollectionFileFlag,
						_akidToPrincipalMapFileFlag,
						_measurementConsumerConfigFileFlag,
						_signingPrivateKeyStoreDirFlag,
						_encryptionKeyPairDirFlag,
						_encryptionKeyPairConfigFileFlag,
						_metricSpecConfigFileFlag,
						_knownEventGroupMetadataTypeFlag,
						"--port=8443",
						"--health-port=8080",
						"--event-group-metadata-descriptor-cache-duration=1h",
						"--certificate-cache-expiration-duration=\(_certificateCacheExpirationDuration)",
						"--data-provider-cache-expiration-duration=\(_dataProviderCacheExpirationDuration)",
			] + _tlsArgs + _internalApiTarget.args + _kingdomApiTarget.args + _accessApiTarget.args

			spec: template: spec: {
				_mounts: {
					"mc-config": {
						volume: secret: secretName: Reporting._mcConfigSecretName
						volumeMount: mountPath: "/var/run/secrets/files/config/mc/"
					}
					"config-files": #ConfigMapMount
				}
				_dependencies: _ | *["postgres-internal-reporting-server", "access-public-api-server"]
			}
		}

		"access-internal-api-server": {
			_container: args: [
						_debugVerboseGrpcServerLoggingFlag,
						_akidToPrincipalMapFileFlag,
						"--cert-collection-file=/var/run/secrets/files/reporting_root.pem",
						"--tls-cert-file=/var/run/secrets/files/access_tls.pem",
						"--tls-key-file=/var/run/secrets/files/access_tls.key",
						"--permissions-config=/etc/\(#AppName)/access-config/permissions_config.textproto",
			] + _accessSpannerConfig.flags

			_updateSchemaContainer: Container=#Container & {
				image:            _images[Container.name]
				args:             _accessSpannerConfig.flags
				imagePullPolicy?: _container.imagePullPolicy
			}

			spec: template: spec: {
				_mounts: {
					"config-files":  #ConfigMapMount
					"access-config": #ConfigMapMount
				}
				_initContainers: {
					"update-access-schema": _updateSchemaContainer
				}
			}
		}

		"access-public-api-server": {
			_container: args: [
						_debugVerboseGrpcClientLoggingFlag,
						_debugVerboseGrpcServerLoggingFlag,
						"--cert-collection-file=/var/run/secrets/files/reporting_root.pem",
						"--tls-cert-file=/var/run/secrets/files/access_tls.pem",
						"--tls-key-file=/var/run/secrets/files/access_tls.key",
			] + _accessInternalApiTarget.args
			spec: template: spec: {
				_dependencies: ["access-internal-api-server"]
			}
		}
	}

	cronJobs: [Name=_]: #CronJob & {
		_name:       Name
		_secretName: Reporting._secretName
		_system:     "reporting"
		_container: {
			image: _images[_name]
		}
	}
	cronJobs: {
		"report-scheduling": {
			_container: args: [
						_debugVerboseGrpcClientLoggingFlag,
						_debugVerboseGrpcServerLoggingFlag,
						_reportingCertCollectionFileFlag,
						_measurementConsumerConfigFileFlag,
						_signingPrivateKeyStoreDirFlag,
						_encryptionKeyPairDirFlag,
						_encryptionKeyPairConfigFileFlag,
						_metricSpecConfigFileFlag,
						"--port=8443",
						"--health-port=8080",
			] + _tlsArgs + _internalApiTarget.args + _kingdomApiTarget.args
			spec: {
				jobTemplate: spec: template: spec: _mounts: {
					"mc-config": {
						volume: secret: secretName: Reporting._mcConfigSecretName
						volumeMount: mountPath: "/var/run/secrets/files/config/mc/"
					}
					"config-files": #ConfigMapMount
				}
				schedule: _reportSchedulingCronSchedule
			}
		}
	}

	networkPolicies: [Name=_]: #NetworkPolicy & {
		_name:      Name
		_app_label: _ | *"\(_name)-app"
	}

	networkPolicies: {
		"postgres-internal-reporting-server": {
			_sourceMatchLabels: [
				"reporting-v2alpha-public-api-server-app",
				"report-scheduling-app",
			]
			_egresses: {
				// Needs to call out to Postgres and Spanner.
				any: {}
			}
		}
		"reporting-v2alpha-public-api-server": {
			_destinationMatchLabels: ["postgres-internal-reporting-server-app"]
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
		"report-scheduling": {
			_destinationMatchLabels: ["postgres-internal-reporting-server-app"]
			_egresses: {
				// Needs to call out to Kingdom.
				any: {}
			}
		}
		"access-internal-api-server": {
			_sourceMatchLabels: ["access-public-api-server-app"]
			_egresses: {
				// Needs to call out to Spanner.
				any: {}
			}
		}
		"access-public-api-server": {
			_sourceMatchLabels: ["reporting-v2alpha-public-api-server-app"]
			_destinationMatchLabels: ["access-internal-api-server-app"]
			_ingresses: {
				gRpc: {
					ports: [{
						port: #GrpcPort
					}]
				}
			}
		}
	}

	configMaps: [Name=string]: #ConfigMap & {
		metadata: name: Name
	}
	configMaps: "access-config": {
		data: {
			"permissions_config.textproto": #PermissionsConfig
		}
	}

	serviceAccounts: [Name=string]: #ServiceAccount & {
		metadata: name: Name
	}
}
