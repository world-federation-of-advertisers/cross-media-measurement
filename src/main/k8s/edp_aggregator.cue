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

let MountRoot = "/etc/\(#AppName)/edp-aggregator"

#EdpAggregator: {

	_verboseGrpcServerLogging: bool | *false
	_verboseGrpcClientLogging: bool | *false

	_spannerConfig: #SpannerConfig

	_edpAggregatorInternalApiTarget: #GrpcTarget & {
		serviceName:           "edp-aggregator-internal-api-server"
		certificateHost:       "localhost"
		targetOption:          "--edp-aggregator-internal-api-target"
		certificateHostOption: "--edp-aggregator-internal-api-cert-host"
	}

	_imageSuffixes: [_=string]: string
	_imageSuffixes: {
		"edp-aggregator-system-api-server":   string | *"edp-aggregator/system-api"
		"edp-aggregator-internal-api-server": string | *"edp-aggregator/internal-api"
		"update-edp-aggregator-schema":       string | *"edp-aggregator/update-schema"
		"sync-event-group-activities":        string | *"edp-aggregator/sync-event-group-activities"
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

	_edpAggregatorSecretName: string

	_debugVerboseGrpcClientLoggingFlag: "--debug-verbose-grpc-client-logging=\(_verboseGrpcClientLogging)"
	_debugVerboseGrpcServerLoggingFlag: "--debug-verbose-grpc-server-logging=\(_verboseGrpcServerLogging)"

	// Schedule for the per-EDP sync-event-group-activities cronjobs. Override in
	// the env-specific overlay (default: daily at 06:00 UTC).
	_syncEventGroupActivitiesCronSchedule: string | *"0 6 * * *"

	// Per-EDP sync-event-group-activities cronjob configs. The overlay sets one
	// entry per EDP. Empty by default — entries produce no cronjob.
	//
	// args: CLI flags passed to the sync-event-group-activities container.
	// tlsSecret: name of the K8s Secret holding this EDP's TLS cert/key
	//   (mounted at MountRoot/<tlsSecret>; cert/key files inside named tls.crt,
	//   tls.key per the secretGenerator convention).
	_syncEventGroupActivitiesArgs: [string]: {
		args: [...string]
		tlsSecret: string
	}

	services: [Name=_]: #GrpcService & {
		metadata: {
			_component: "edp-aggregator"
			name:       Name
		}
	}

	services: {
		"edp-aggregator-internal-api-server": {}
		"edp-aggregator-system-api-server": #ExternalService
	}

	deployments: [Name=_]: #ServerDeployment & {
		_name:       Name
		_secretName: _edpAggregatorSecretName
		_system:     "edp-aggregator"
		_container: {
			image: _images[_name]
		}
	}
	deployments: {
		"edp-aggregator-internal-api-server": {
			_container: args: [
						_debugVerboseGrpcServerLoggingFlag,
						"--cert-collection-file=\(MountRoot)/config/trusted_certs.pem",
						"--tls-cert-file=\(MountRoot)/tls/tls.crt",
						"--tls-key-file=\(MountRoot)/tls/tls.key",
			] + _spannerConfig.flags

			_updateSchemaContainer: Container=#Container & {
				image:            _images[Container.name]
				args:             _spannerConfig.flags
				imagePullPolicy?: _container.imagePullPolicy
			}

			spec: template: spec: {
				_initContainers: {
					"update-edp-aggregator-schema": _updateSchemaContainer
				}
				_mounts: {
					"edp-aggregator-tls": #SecretMount & {
						volumeMount: mountPath: "\(MountRoot)/tls"
					}
					"edp-aggregator-config": #ConfigMapMount & {
						volumeMount: mountPath: "\(MountRoot)/config"
					}
				}
			}
		}

		"edp-aggregator-system-api-server": {
			_container: args: [
						_debugVerboseGrpcClientLoggingFlag,
						_debugVerboseGrpcServerLoggingFlag,
						"--cert-collection-file=\(MountRoot)/config/trusted_certs.pem",
						"--tls-cert-file=\(MountRoot)/tls/tls.crt",
						"--tls-key-file=\(MountRoot)/tls/tls.key",
			] + _edpAggregatorInternalApiTarget.args
			spec: template: spec: {
				_dependencies: ["edp-aggregator-internal-api-server"]
				_mounts: {
					"edp-aggregator-tls": #SecretMount & {
						volumeMount: mountPath: "\(MountRoot)/tls"
					}
					"edp-aggregator-config": #ConfigMapMount & {
						volumeMount: mountPath: "\(MountRoot)/config"
					}
				}
			}
		}

	}

	networkPolicies: [Name=_]: #NetworkPolicy & {
		_name:      Name
		_app_label: _ | *"\(_name)-app"
	}

	networkPolicies: {
		"edp-aggregator-internal-api-server": {
			_sourceMatchLabels: ["edp-aggregator-system-api-server-app"]
			_egresses: {
				// Needs to call out to Spanner.
				any: {}
			}
		}
		"edp-aggregator-system-api-server": {
			_destinationMatchLabels: ["edp-aggregator-internal-api-server-app"]
			_ingresses: {
				gRpc: {
					ports: [{
						port: #GrpcPort
					}]
				}
			}
		}
		for edp, _ in _syncEventGroupActivitiesArgs {
			"sync-event-group-activities-\(edp)": {
				_app_label: "sync-event-group-activities-\(edp)-app"
				_egresses: {
					// Needs to call out to GCS (read spot-data input) and the
					// Kingdom public API (list/create/delete EventGroupActivities).
					any: {}
				}
			}
		}
	}

	// K8s ServiceAccount that the CronJob pods run as. Set in the overlay
	// (e.g. dev/edp_aggregator_gke.cue) and bound via Workload Identity to a
	// GCP SA that has storage.objectViewer on the spot-data bucket.
	_syncEventGroupActivitiesServiceAccountName: string

	cronJobs: [Name=_]: #CronJob & {
		_name:       Name
		_secretName: _edpAggregatorSecretName
		_system:     "edp-aggregator"
		_container: {
			image: _images["sync-event-group-activities"]
			_javaOptions: maxHeapSize: "256M"
			resources: {
				requests: {
					cpu:    "100m"
					memory: "512Mi"
				}
				limits: {
					memory: "512Mi"
				}
			}
		}
		spec: {
			concurrencyPolicy: "Forbid"
			schedule:          _syncEventGroupActivitiesCronSchedule
			jobTemplate: spec: template: spec: #ServiceAccountPodSpec & {
				serviceAccountName: _syncEventGroupActivitiesServiceAccountName
				// Per-EDP secret mounts (e.g. edp7-tls) are added by the per-EDP
				// override block below; only the shared ConfigMap mount lives here.
				_mounts: {
					"edp-aggregator-config": #ConfigMapMount & {
						volumeMount: mountPath: "\(MountRoot)/config"
					}
				}
			}
		}
	}
	cronJobs: {
		for edp, cfg in _syncEventGroupActivitiesArgs {
			"sync-event-group-activities-\(edp)": {
				_container: args: cfg.args
				spec: jobTemplate: spec: template: spec: _mounts: {
					"\(cfg.tlsSecret)": #SecretMount & {
						volumeMount: mountPath: "\(MountRoot)/\(cfg.tlsSecret)"
					}
				}
			}
		}
	}

	configMaps: [Name=string]: #ConfigMap & {
		metadata: name: Name
	}

	serviceAccounts: [Name=string]: #ServiceAccount & {
		metadata: name: Name
	}
}
