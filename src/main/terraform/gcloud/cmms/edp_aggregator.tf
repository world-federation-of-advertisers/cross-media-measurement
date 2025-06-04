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

  edp_display_names = ["epd7"]

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
    secret_id         = "edpa-datawatcher-tls-key"
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/edpa_tee_app_tls.key"),
    is_binary_format  = false
  }

  data_watcher_tls_pem = {
    secret_id         = "edpa-datawatcher-tls-pem"
    secret_local_path = abspath("${path.root}/../../../k8s/testing/secretfiles/data_watcher_tls.pem"),
    is_binary_format  = false
  }

  secure_computation_root_ca = {
    secret_id         = "secure-computation-root-ca"
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
    subscription_name     = "requisition-fulfiller-subscription"
    topic_name            = "requisition-fulfiller-queue"
      ack_deadline_seconds  = 600
    }
    worker = {
      instance_template_name        = "requisition-fulfiller-template"
      base_instance_name            = "secure-computation"
      managed_instance_group_name   = "requisition-fulfiller-mig"
      mig_service_account_name      = "requisition-fulfiller-sa"
      single_instance_assignment    = 1
      min_replicas                  = 1
      max_replicas                  = 10
      app_args = [
        "--kingdom-public-api-target=${var.kingdom_public_api_target}",
        "--secure-computation-public-api-target=${var.secure_computation_public_api_target}",
        "--subscription-id=requisition-fulfiller-subscription",
        "--google-pub-sub-project-id=${data.google_client_config.default.project}"
      ]
      machine_type                  = "n2d-standard-2"
      docker_image                  = "ghcr.io/world-federation-of-advertisers/edp-aggregator/results_fulfiller:${var.image_tag}"
      mig_distribution_policy_zones = ["us-central1-a"]
    }
  }
}

module "edp_aggregator" {
  source = "../modules/edp-aggregator"

  key_ring_name                             = "edpa-secure-computation-cloud-test-key-ring"
  key_ring_location                         = local.key_ring_location
  kms_key_name                              = "edpa-secure-computation-kek"
  requisition_fulfiller_config              = local.requisition_fulfiller_config
  pubsub_iam_service_account_member         = module.secure_computation.secure_computation_internal_iam_service_account_member
  edp_aggregator_bucket_name                = var.secure_computation_storage_bucket_name
  edp_aggregator_bucket_location            = local.storage_bucket_location
  data_watcher_service_account_name         = "edpa-data-watcher"
  data_watcher_trigger_service_account_name = "edpa-data-watcher-trigger"
  terraform_service_account                 = var.terraform_service_account
  requisition_fetcher_service_account_name  = "edpa-requisition-fetcher"
  event_group_sync_service_account_name     = "edpa-event-group-sync"
  event_group_sync_function_name            = "event-group-sync"
  edpa_tee_app_tls_key                      = local.edpa_tee_app_tls_key
  edpa_tee_app_tls_pem                      = local.edpa_tee_app_tls_pem
  data_watcher_tls_key                      = local.data_watcher_tls_key
  data_watcher_tls_pem                      = local.data_watcher_tls_pem
  secure_computation_root_ca                = local.secure_computation_root_ca
  kingdom_root_ca                           = local.kingdom_root_ca
  edps_certs                                = local.edps_certs

}
