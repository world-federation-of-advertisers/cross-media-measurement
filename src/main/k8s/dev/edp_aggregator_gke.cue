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

// Per-EDP sync-event-group-activities config (one set per EDP). Empty by
// default so envs without the spot-data pipeline produce no CronJob.
_syncEventGroupActivitiesEdp7DataProvider: string | *"" @tag("sync_event_group_activities_edp7_data_provider")
_syncEventGroupActivitiesEdp7BlobUri:      string | *"" @tag("sync_event_group_activities_edp7_blob_uri")
_syncEventGroupActivitiesGcsProject:       string | *"" @tag("sync_event_group_activities_gcs_project")
_kingdomPublicApiTarget:                   string | *"" @tag("kingdom_public_api_target")

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

	// Per-EDP arg lists for sync-event-group-activities. Values flow from
	// environment variables (set per env in GitHub Settings -> Environments ->
	// <env> -> Variables) so the same overlay deploys cleanly across dev, head,
	// and qa. An EDP entry is generated only when its data_provider tag is
	// non-empty, so envs that have not configured the spot-data pipeline yet
	// produce no CronJob for that EDP. Schedule defaults to daily at 06:00 UTC
	// (see edp_aggregator.cue _syncEventGroupActivitiesCronSchedule).
	if _syncEventGroupActivitiesEdp7DataProvider != "" {
		_syncEventGroupActivitiesArgs: "edp7": [
			"--data-provider=\(_syncEventGroupActivitiesEdp7DataProvider)",
			"--blob-uri=\(_syncEventGroupActivitiesEdp7BlobUri)",
			"--gcs-project-id=\(_syncEventGroupActivitiesGcsProject)",
			"--kingdom-public-api-target=\(_kingdomPublicApiTarget)",
			"--kingdom-public-api-cert-host=localhost",
			"--tls-cert-file=/var/run/secrets/files/edp7_tls.pem",
			"--tls-key-file=/var/run/secrets/files/edp7_tls.key",
			"--cert-collection-file=/var/run/secrets/files/kingdom_root.pem",
			"--list-page-size=1000",
			"--throttler-minimum-interval=100ms",
			"--dry-run",
		]
	}
}
