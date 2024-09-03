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

_secret_name:          string @tag("secret_name")
_aggregator_cert_name: string @tag("aggregator_cert_name")
_duchyDbSecretName:    string @tag("db_secret_name")
_worker1_cert_name:    string @tag("worker1_cert_name")
_worker2_cert_name:    string @tag("worker2_cert_name")

#KingdomSystemApiTarget: (#Target & {name: "system-api-server"}).target
#KingdomPublicApiTarget: (#Target & {name: "v2alpha-public-api-server"}).target
#SpannerEmulatorHost:    (#Target & {name: "spanner-emulator"}).target

#DuchyConfig: {
	let duchyName = name
	name:                            string
	databaseType:                    string
	protocolsSetupConfig:            string
	certificateResourceName:         string
	computationControlServiceTarget: (#Target & {name: "\(duchyName)-computation-control-server"}).target
}
_duchyConfigs: [Name=_]: #DuchyConfig & {
	name: Name
}
_duchyConfigs: {
	"aggregator": {
		protocolsSetupConfig:    "aggregator_protocols_setup_config.textproto"
		certificateResourceName: _aggregator_cert_name
		databaseType:            "spanner"
	}
	"worker1": {
		protocolsSetupConfig:      "worker1_protocols_setup_config.textproto"
		certificateResourceName:   _worker1_cert_name
		databaseType:              "spanner"
		duchyKeyEncryptionKeyFile: "worker1_kek.tink"
	}
	"worker2": {
		protocolsSetupConfig:      "worker2_protocols_setup_config.textproto"
		certificateResourceName:   _worker2_cert_name
		databaseType:              "postgres"
		duchyKeyEncryptionKeyFile: "worker2_kek.tink"
	}
}

objectSets: [ for duchy in duchies for objectSet in duchy {objectSet}]

_computationControlTargets: {
	for name, duchyConfig in _duchyConfigs {
		"\(name)": duchyConfig.computationControlServiceTarget
	}
}

#Duchy: {
	_imageSuffixes: {
		"computation-control-server":     "duchy/local-computation-control"
		"herald-daemon":                  "duchy/local-herald"
		"llv2-mill":                      "duchy/local-liquid-legions-v2-mill"
		"hmss-mill":                      "duchy/local-honest-majority-share-shuffle-mill"
		"requisition-fulfillment-server": "duchy/local-requisition-fulfillment"
	}
	_duchy_secret_name:           _secret_name
	_computation_control_targets: _computationControlTargets
	_kingdom_system_api_target:   #KingdomSystemApiTarget
	_kingdom_public_api_target:   #KingdomPublicApiTarget
	_blob_storage_flags: [
		"--forwarded-storage-service-target=" + (#Target & {name: "fake-storage-server"}).target,
		"--forwarded-storage-cert-host=localhost",
	]
	_verbose_grpc_logging: "true"
}

#SpannerDuchy: {
	_imageSuffixes: {
		"internal-api-server": "duchy/local-spanner-computations"
	}
}

#PostgresDuchy: {
	_imageSuffixes: {
		"internal-api-server": "duchy/local-postgres-internal-server"
	}
	_postgresConfig: {
		serviceName: "postgres"
		password:    "$(POSTGRES_PASSWORD)"
		user:        "$(POSTGRES_USER)"
	}
	deployments: {
		"internal-api-server-deployment": {
			let EnvVars = #EnvVarMap & {
				"POSTGRES_USER": {
					valueFrom:
						secretKeyRef: {
							name: _duchyDbSecretName
							key:  "username"
						}
				}
				"POSTGRES_PASSWORD": {
					valueFrom:
						secretKeyRef: {
							name: _duchyDbSecretName
							key:  "password"
						}
				}
			}

			_container: _envVars:             EnvVars
			_updateSchemaContainer: _envVars: EnvVars
		}
	}
}

duchies: [
	for duchyConfig in _duchyConfigs {
		{
			_duchy: {
				name:                      duchyConfig.name
				protocols_setup_config:    duchyConfig.protocolsSetupConfig
				cs_cert_resource_name:     duchyConfig.certificateResourceName
				duchyKeyEncryptionKeyFile: duchyConfig.duchyKeyEncryptionKeyFile
			}

			if (duchyConfig.databaseType == "spanner") {
				#SpannerDuchy
			}
			if (duchyConfig.databaseType == "postgres") {
				#PostgresDuchy
			}
		}
	},
]
