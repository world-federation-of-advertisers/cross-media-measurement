# Copyright 2025 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

locals {

  edp_display_names = ["edp7", "edpa_meta"]

  edpa_tee_app_tls_key = {
    secret_id         = "edpa-tee-app-tls-key",
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/edpa_tee_app_tls.key"),
    is_binary_format  = false
  }

  edpa_tee_app_tls_pem = {
    secret_id         = "edpa-tee-app-tls-pem"
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/edpa_tee_app_tls.pem"),
    is_binary_format  = false
  }

  data_watcher_tls_key = {
    secret_id         = "edpa-data-watcher-tls-key"
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/data_watcher_tls.key"),
    is_binary_format  = false
  }

  data_watcher_tls_pem = {
    secret_id         = "edpa-data-watcher-tls-pem"
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/data_watcher_tls.pem"),
    is_binary_format  = false
  }

  data_availability_tls_key = {
    secret_id         = "edpa-data-availability-tls-key"
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/data_availability_tls.key"),
    is_binary_format  = false
  }

  data_availability_tls_pem = {
    secret_id         = "edpa-data-availability-tls-pem"
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/data_availability_tls.pem"),
    is_binary_format  = false
  }

  requisition_fetcher_tls_key = {
    secret_id         = "edpa-requisition-fetcher-tls-key"
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/requisition_fetcher_tls.key"),
    is_binary_format  = false
  }

  requisition_fetcher_tls_pem = {
    secret_id         = "edpa-requisition-fetcher-tls-pem"
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/requisition_fetcher_tls.pem"),
    is_binary_format  = false
  }

  secure_computation_root_ca = {
    secret_id         = "securecomputation-root-ca"
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/secure_computation_root.pem"),
    is_binary_format  = false
  }

  metadata_storage_root_ca = {
      secret_id         = "edpaggregator-root-ca"
      secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/edp_aggregator_root.pem"),
      is_binary_format  = false
    }

  trusted_root_ca_collection = {
    secret_id         = "trusted-root-ca"
    secret_local_path = var.results_fulfiller_trusted_root_ca_collection_file_path
    is_binary_format  = false
  }

  edps_certs = {
    for edp in local.edp_display_names : edp => {
      cert_der = {
        secret_id         = "${edp}-cert-der"
        secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/${edp}_cs_cert.der")
        is_binary_format  = true
      }
      private_der = {
        secret_id         = "${edp}-private-der"
        secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/${edp}_cs_private.der")
        is_binary_format  = true
      }
      enc_private = {
        secret_id         = "${edp}-enc-private"
        secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/${edp}_enc_private.tink")
        is_binary_format  = true
      }
      tls_key = {
        secret_id         = "${edp}-tls-key"
        secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/${edp}_tls.key")
        is_binary_format  = false
      }
      tls_pem = {
        secret_id         = "${edp}-tls-pem"
        secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/${edp}_tls.pem")
        is_binary_format  = false
      }
    }
  }

  requisition_fulfiller_config = {
    queue = {
    subscription_name     = "results-fulfiller-subscription"
    topic_name            = "results-fulfiller-queue"
      ack_deadline_seconds  = 600
    }
    worker = {
      instance_template_name        = "requisition-fulfiller-template"
      base_instance_name            = "secure-computation"
      managed_instance_group_name   = "results-fulfiller-mig-v2"
      mig_service_account_name      = "results-fulfiller-sa"
      single_instance_assignment    = 1
      min_replicas                  = 0
      max_replicas                  = 10
      machine_type                  = "c4d-standard-32"
      java_tool_options             = "-Xmx96G"
      docker_image                  = "ghcr.io/world-federation-of-advertisers/edp-aggregator/results_fulfiller:${var.image_tag}"
      mig_distribution_policy_zones = ["us-central1-a"]
      app_flags                     = [
                                          "--edpa-tls-cert-secret-id", "edpa-tee-app-tls-pem",
                                          "--edpa-tls-cert-file-path", "/tmp/edpa_certs/edpa_tee_app_tls.pem",
                                          "--edpa-tls-key-secret-id", "edpa-tee-app-tls-key",
                                          "--edpa-tls-key-file-path", "/tmp/edpa_certs/edpa_tee_app_tls.key",
                                          "--secure-computation-cert-collection-secret-id", "securecomputation-root-ca",
                                          "--secure-computation-cert-collection-file-path", "/tmp/edpa_certs/secure_computation_root.pem",
                                          "--metadata-storage-cert-collection-secret-id", "edpaggregator-root-ca",
                                          "--metadata-storage-cert-collection-file-path", "/tmp/edpa_certs/edp_aggregator_root.pem",
                                          "--trusted-cert-collection-secret-id", "trusted-root-ca",
                                          "--trusted-cert-collection-file-path", "/tmp/edpa_certs/trusted_root.pem",
                                          "--kingdom-public-api-target", var.kingdom_public_api_target,
                                          "--secure-computation-public-api-target", var.secure_computation_public_api_target,
                                          "--metadata-storage-public-api-target", var.metadata_storage_public_api_target,
                                          "--subscription-id", "results-fulfiller-subscription",
                                          "--google-project-id", data.google_client_config.default.project,
                                          "--model-line", var.edpa_model_line_map,
                                          "--population-spec-file-blob-uri", var.results_fulfiller_population_spec_blob_uri,
                                          "--event-template-descriptor-blob-uri", var.results_fulfiller_event_proto_descriptor_blob_uri,
                                          "--event-template-type-name", var.results_fulfiller_event_template_type_name,
                                          "--duchy-id", var.duchy_worker1_id,
                                          "--duchy-target", var.duchy_worker1_target,
                                          "--duchy-id", var.duchy_worker2_id,
                                          "--duchy-target", var.duchy_worker2_target,
                                        ]
    }
  }

  requisition_fetcher_scheduler_config = {
    schedule                    = "*/15 * * * *"  # Every 15 minutes
    time_zone                   = "UTC"
    name                        = "requisition-fetcher-scheduler"
    function_url                = "https://${data.google_client_config.default.region}-${data.google_client_config.default.project}.cloudfunctions.net/requisition-fetcher"
    scheduler_sa_display_name   = "Requisition Fetcher Scheduler"
    scheduler_sa_description    = "Service account for Cloud Scheduler to trigger requisition fetcher"
    scheduler_job_description   = "Scheduled job to fetch unfulfilled requisitions from the Kingdom"
  }

  data_watcher_config = {
    local_path  = var.data_watcher_config_file_path
    destination = "data-watcher-config.textproto"
  }

  requisition_fetcher_config = {
    local_path  = var.requisition_fetcher_config_file_path
    destination = "requisition-fetcher-config.textproto"
  }

  edps_config = {
      local_path  = var.event_data_provider_configs_file_path
      destination = "event-data-provider-configs.textproto"
    }

  results_fulfiller_event_descriptor = {
    local_path  = var.results_fulfiller_event_proto_descriptor_path
    destination = "results_fulfiller_event_proto_descriptor.pb"
  }

  results_fulfiller_population_spec = {
    local_path  = var.results_fulfiller_population_spec_file_path
    destination = "results-fulfiller-population-spec.textproto"
  }

  cloud_function_configs = {
    data_watcher = {
      function_name       = "data-watcher"
      entry_point         = "org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.DataWatcherFunction"
      extra_env_vars      = var.data_watcher_env_var
      secret_mappings     = var.data_watcher_secret_mapping
      uber_jar_path       = var.data_watcher_uber_jar_path
    },
    requisition_fetcher = {
      function_name       = "requisition-fetcher"
      entry_point         = "org.wfanet.measurement.edpaggregator.deploy.gcloud.requisitionfetcher.RequisitionFetcherFunction"
      extra_env_vars      = var.requisition_fetcher_env_var
      secret_mappings     = var.requisition_fetcher_secret_mapping
      uber_jar_path       = var.requisition_fetcher_uber_jar_path
    },
    event_group_sync = {
      function_name       = "event-group-sync"
      entry_point         = "org.wfanet.measurement.edpaggregator.deploy.gcloud.eventgroups.EventGroupSyncFunction"
      extra_env_vars      = var.event_group_env_var
      secret_mappings     = var.event_group_secret_mapping
      uber_jar_path       = var.event_group_uber_jar_path
    }
    data_availability_sync = {
      function_name       = "data-availability-sync"
      entry_point         = "org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability.DataAvailabilitySyncFunction"
      extra_env_vars      = var.data_availability_env_var
      secret_mappings     = var.data_availability_secret_mapping
      uber_jar_path       = var.data_availability_uber_jar_path
    }
  }

}

module "edp_aggregator" {
  source = "../modules/edp-aggregator"

  requisition_fulfiller_config                  = local.requisition_fulfiller_config
  pubsub_iam_service_account_member             = module.secure_computation.secure_computation_internal_iam_service_account_member
  edp_aggregator_bucket_name                    = var.secure_computation_storage_bucket_name
  config_files_bucket_name                      = var.edpa_config_files_bucket_name
  edp_aggregator_buckets_location               = local.storage_bucket_location
  data_watcher_service_account_name             = "edpa-data-watcher"
  data_watcher_trigger_service_account_name     = "edpa-data-watcher-trigger"
  terraform_service_account                     = var.terraform_service_account
  requisition_fetcher_service_account_name      = "edpa-requisition-fetcher"
  data_availability_sync_service_account_name   = "edpa-data-availability-sync"
  data_watcher_config                           = local.data_watcher_config
  requisition_fetcher_config                    = local.requisition_fetcher_config
  edps_config                                   = local.edps_config
  results_fulfiller_event_descriptor            = local.results_fulfiller_event_descriptor
  results_fulfiller_population_spec             = local.results_fulfiller_population_spec
  event_group_sync_service_account_name         = "edpa-event-group-sync"
  event_group_sync_function_name                = "event-group-sync"
  data_availability_sync_function_name          = "data-availability-sync"
  edpa_tee_app_tls_key                          = local.edpa_tee_app_tls_key
  edpa_tee_app_tls_pem                          = local.edpa_tee_app_tls_pem
  data_watcher_tls_key                          = local.data_watcher_tls_key
  data_watcher_tls_pem                          = local.data_watcher_tls_pem
  data_availability_tls_key                     = local.data_availability_tls_key
  data_availability_tls_pem                     = local.data_availability_tls_pem
  requisition_fetcher_tls_key                   = local.requisition_fetcher_tls_key
  requisition_fetcher_tls_pem                   = local.requisition_fetcher_tls_pem
  secure_computation_root_ca                    = local.secure_computation_root_ca
  metadata_storage_root_ca                      = local.metadata_storage_root_ca
  trusted_root_ca_collection                    = local.trusted_root_ca_collection
  edps_certs                                    = local.edps_certs
  requisition_fetcher_scheduler_config          = local.requisition_fetcher_scheduler_config
  cloud_function_configs                        = local.cloud_function_configs
  results_fulfiller_disk_image_family           = "confidential-space"
  dns_managed_zone_name                         = "googleapis-private"
  edp_aggregator_service_account_name           = "edp-aggregator-internal"
  spanner_instance                              = google_spanner_instance.spanner_instance
}
