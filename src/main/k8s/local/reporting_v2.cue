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

_pdpName:                      string @tag("pdp_name")
_reportingBasicReportsEnabled: string @tag("basic_reports_enabled")
_reportingSecretName:          string @tag("secret_name")
_reportingDbSecretName:        string @tag("db_secret_name")
_reportingMcConfigSecretName:  string @tag("mc_config_secret_name")

objectSets: [ for objectSet in reporting {objectSet}]

reporting: #Reporting & {
	_populationDataProviderName: _pdpName
	_basicReportsEnabled:        _reportingBasicReportsEnabled
	_secretName:                 _reportingSecretName
	_mcConfigSecretName:         _reportingMcConfigSecretName
	_postgresConfig: {
		serviceName:      "postgres"
		password:         "$(POSTGRES_PASSWORD)"
		user:             "$(POSTGRES_USER)"
		statementTimeout: "60s"
	}
	_kingdomApiTarget: {
		serviceName:     "v2alpha-public-api-server"
		certificateHost: "localhost"
	}
	_verboseGrpcServerLogging: true
	_verboseGrpcClientLogging: true
	_eventMessageTypeUrl:      "type.googleapis.com/wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"

	let EnvVars = #EnvVarMap & {
		"POSTGRES_USER": {
			valueFrom:
				secretKeyRef: {
					name: _reportingDbSecretName
					key:  "username"
				}
		}
		"POSTGRES_PASSWORD": {
			valueFrom:
				secretKeyRef: {
					name: _reportingDbSecretName
					key:  "password"
				}
		}
	}

	deployments: {
		"postgres-internal-reporting-server": {
			_container: _envVars:                     EnvVars
			_updatePostgresSchemaContainer: _envVars: EnvVars
			spec: template: spec: {
				_dependencies: ["spanner-emulator"]
			}
		}
		"reporting-v2alpha-public-api-server": {
			spec: template: spec: {
				_dependencies: [
					"postgres-internal-reporting-server",
					"access-public-api-server",
					"v2alpha-public-api-server", // Kingdom public API server.
				]
			}
		}
	}
}
