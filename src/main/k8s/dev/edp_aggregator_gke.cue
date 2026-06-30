// Copyright 2025 The Cross-Media Measurement Authors
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

_secretName:           string @tag("secret_name")
_systemApiAddressName: "edp-aggregator-system"

// Name of K8s service account for the internal API server.
#InternalEdpAggregatorServerServiceAccount: "internal-edp-aggregator-server"

#SystemServerResourceRequirements: ResourceRequirements=#ResourceRequirements & {
	requests: {
		cpu:    "25m"
		memory: "256Mi"
	}
	limits: {
		memory: ResourceRequirements.requests.memory
	}
}

objectSets: [
	defaultNetworkPolicies,
	edpAggregator.serviceAccounts,
	edpAggregator.configMaps,
	edpAggregator.deployments,
	edpAggregator.services,
	edpAggregator.networkPolicies,
	edpAggregator.cronJobs,
]

edpAggregator: #EdpAggregator & {

	_edpAggregatorSecretName:  _secretName
	_verboseGrpcServerLogging: true

	_spannerConfig: database: "edp-aggregator"

	serviceAccounts: {
		"\(#InternalEdpAggregatorServerServiceAccount)": #WorkloadIdentityServiceAccount & {
			_iamServiceAccountName: "edp-aggregator-internal"
		}
	}

	configMaps: "java": #JavaConfigMap

	deployments: {
		"edp-aggregator-internal-api-server": {
			spec: template: spec: #ServiceAccountPodSpec & {
				serviceAccountName: #InternalEdpAggregatorServerServiceAccount
			}
		}
		"edp-aggregator-system-api-server": {
			_container: resources: #SystemServerResourceRequirements
		}
	}
	services: {
		"edp-aggregator-system-api-server": _ipAddressName: _systemApiAddressName
	}

	// Per-EDP arg lists for sync-event-group-activities. Each EDP has an entry
	// here; the textproto config for that EDP is staged into the edp-aggregator
	// ConfigMap by the deploy workflow and mounted at /etc/halo-cmm/edp-aggregator/config/.
	// Schedule defaults to daily at 06:00 UTC (see edp_aggregator.cue
	// _syncEventGroupActivitiesCronSchedule).
	_syncEventGroupActivitiesArgs: "edp7": [
		"--config-file=/etc/halo-cmms/edp-aggregator/config/event-group-activity-sync-config-edp7.textproto",
		"--tls-cert-file=/etc/halo-cmms/edp-aggregator/edp7-tls/tls.crt",
		"--tls-key-file=/etc/halo-cmms/edp-aggregator/edp7-tls/tls.key",
		"--cert-collection-file=/etc/halo-cmms/edp-aggregator/config/trusted_certs.pem",
		"--list-page-size=1000",
		"--throttler-minimum-interval=100ms",
		"--dry-run",
	]
}
