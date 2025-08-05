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

  edp_display_names = ["edp7"]

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

  secure_computation_root_ca = {
    secret_id         = "securecomputation-root-ca"
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/secure_computation_root.pem"),
    is_binary_format  = false
  }

  kingdom_root_ca = {
    secret_id         = "kingdom-root-ca"
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/kingdom_root.pem"),
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
      managed_instance_group_name   = "results-fulfiller-mig"
      mig_service_account_name      = "results-fulfiller-sa"
      single_instance_assignment    = 1
      min_replicas                  = 1
      max_replicas                  = 10
      machine_type                  = "n2d-standard-2"
      docker_image                  = "ghcr.io/world-federation-of-advertisers/edp-aggregator/results_fulfiller:${var.image_tag}"
      mig_distribution_policy_zones = ["us-central1-a"]
      app_flags                     = [
                                          "--edpa-tls-cert-secret-id", "edpa-tee-app-tls-pem",
                                          "--edpa-tls-key-secret-id", "edpa-tee-app-tls-key",
                                          "--secure-computation-cert-collection-secret-id", "securecomputation-root-ca",
                                          "--kingdom-cert-collection-secret-id", "kingdom-root-ca",
                                          "--edp-cert-der-secret-id", "edp7-cert-der",
                                          "--edp-cert-der-file-path", "/tmp/edp_certs/edp7_cs_cert.der",
                                          "--edp-private-der-secret-id", "edp7-private-der",
                                          "--edp-private-der-file-path", "/tmp/edp_certs/edp7_cs_private.der",
                                          "--edp-enc-private-secret-id", "edp7-enc-private",
                                          "--edp-enc-private-file-path", "/tmp/edp_certs/edp7_enc_private.tink",
                                          "--edp-tls-key-secret-id", "edp7-tls-key",
                                          "--edp-tls-key-file-path", "/tmp/edp_certs/edp7_tls.key",
                                          "--edp-tls-pem-secret-id", "edp7-tls-pem",
                                          "--edp-tls-pem-file-path", "/tmp/edp_certs/edp7_tls.pem",
                                          "--kingdom-public-api-target", var.kingdom_public_api_target,
                                          "--secure-computation-public-api-target", var.secure_computation_public_api_target,
                                          "--subscription-id", "results-fulfiller-subscription",
                                          "--google-project-id", data.google_client_config.default.project,
                                          "--event-template-metadata-blob-uri", "gs://edpa-configs-storage-dev-bucket/results_fulfiller_event_proto_descriptor.pb"
                                        ]
    }
  }

  data_watcher_config = {
    local_path  = var.data_watcher_config_file_path
    destination = "data-watcher-config.textproto"
  }

  requisition_fetcher_config = {
    local_path  = var.requisition_fetcher_config_file_path
    destination = "requisition-fetcher-config.textproto"
  }

  results_fulfiller_config = {
    local_path  = var.results_fulfiller_event_proto_descriptor_path
    destination = "results_fulfiller_event_proto_descriptor.pb"
  }

  cloud_function_configs = {
    data_watcher = {
      function_name       = var.data_watcher_function_name
      entry_point         = "org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.DataWatcherFunction"
      extra_env_vars      = var.data_watcher_env_var
      secret_mappings     = var.data_watcher_secret_mapping
      uber_jar_path       = var.data_watcher_uber_jar_path
    },
    requisition_fetcher = {
      function_name       = var.requisition_fetcher_function_name
      entry_point         = "org.wfanet.measurement.edpaggregator.deploy.gcloud.requisitionfetcher.RequisitionFetcherFunction"
      extra_env_vars      = var.requisition_fetcher_env_var
      secret_mappings     = var.requisition_fetcher_secret_mapping
      uber_jar_path       = var.requisition_fetcher_uber_jar_path
    },
    event_group_sync = {
      function_name       = var.event_group_sync_function_name
      entry_point         = "org.wfanet.measurement.edpaggregator.deploy.gcloud.eventgroups.EventGroupSyncFunction"
      extra_env_vars      = var.event_group_env_var
      secret_mappings     = var.requisition_fetcher_secret_mapping
      uber_jar_path       = var.event_group_uber_jar_path
    }
  }

}

module "edp_aggregator" {
  source = "../modules/edp-aggregator"

  requisition_fulfiller_config              = local.requisition_fulfiller_config
  pubsub_iam_service_account_member         = module.secure_computation.secure_computation_internal_iam_service_account_member
  edp_aggregator_bucket_name                = var.secure_computation_storage_bucket_name
  config_files_bucket_name                  = var.edpa_config_files_bucket_name
  edp_aggregator_buckets_location           = local.storage_bucket_location
  data_watcher_service_account_name         = "edpa-data-watcher"
  data_watcher_trigger_service_account_name = "edpa-data-watcher-trigger"
  terraform_service_account                 = var.terraform_service_account
  requisition_fetcher_service_account_name  = "edpa-requisition-fetcher"
  data_watcher_config                       = local.data_watcher_config
  requisition_fetcher_config                = local.requisition_fetcher_config
  results_fulfiller_config                  = local.results_fulfiller_config
  event_group_sync_service_account_name     = "edpa-event-group-sync"
  event_group_sync_function_name            = "event-group-sync"
  edpa_tee_app_tls_key                      = local.edpa_tee_app_tls_key
  edpa_tee_app_tls_pem                      = local.edpa_tee_app_tls_pem
  data_watcher_tls_key                      = local.data_watcher_tls_key
  data_watcher_tls_pem                      = local.data_watcher_tls_pem
  secure_computation_root_ca                = local.secure_computation_root_ca
  kingdom_root_ca                           = local.kingdom_root_ca
  edps_certs                                = local.edps_certs
  cloud_function_configs                    = local.cloud_function_configs
}
