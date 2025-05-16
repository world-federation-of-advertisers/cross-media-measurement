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
  secrets = {
    edpa_tee_app_tls_key = {
      secret_id         = "edpa-tee-app-tls-key-2"
      secret_local_path = "${path.root}/../../../k8s/testing/secretfiles/edpa_tee_app_tls.key"
      is_binary_format  = false
    }
    edpa_tee_app_tls_pem = {
      secret_id         = "edpa-tee-app-tls-pem-2"
      secret_local_path = "${path.root}/../../../k8s/testing/secretfiles/edpa_tee_app_tls.pem"
      is_binary_format  = false
    }
    data_watcher_tls_key = {
      secret_id         = "edpa-datawatcher-tls-key-2"
      secret_local_path = "${path.root}/../../../k8s/testing/secretfiles/data_watcher_tls.key"
      is_binary_format  = false
    }
    data_watcher_tls_pem = {
      secret_id         = "edpa-datawatcher-tls-pem-2"
      secret_local_path = "${path.root}/../../../k8s/testing/secretfiles/data_watcher_tls.pem"
      is_binary_format  = false
    }
    secure_computation_root_ca = {
      secret_id         = "secure-computation-root-ca-2"
      secret_local_path = "${path.root}/../../../k8s/testing/secretfiles/secure_computation_root.pem"
      is_binary_format  = false
    }
    kingdom_root_ca = {
      secret_id         = "kingdom-root-ca-2"
      secret_local_path = "${path.root}/../../../k8s/testing/secretfiles/kingdom_root.pem"
      is_binary_format  = false
    }
    edp7_cert_der = {
      secret_id         = "edp7-cert-der-2"
      secret_local_path = "${path.root}/../../../k8s/testing/secretfiles/edp7_cs_cert.der"
      is_binary_format  = true
    }
    edp7_private_der = {
      secret_id         = "edp7-private-der-2"
      secret_local_path = "${path.root}/../../../k8s/testing/secretfiles/edp7_cs_private.der"
      is_binary_format  = true
    }
    edp7_enc_private = {
      secret_id         = "edp7-enc-private-2"
      secret_local_path = "${path.root}/../../../k8s/testing/secretfiles/edp7_enc_private.tink"
      is_binary_format  = true
    }
    edp7_tls_key = {
      secret_id         = "edp7-tls-key-2"
      secret_local_path = "${path.root}/../../../k8s/testing/secretfiles/edp7_tls.key"
      is_binary_format  = false
    }
    edp7_tls_pem = {
      secret_id         = "edp7-tls-pem-2"
      secret_local_path = "${path.root}/../../../k8s/testing/secretfiles/edp7_tls.pem"
      is_binary_format  = false
    }
  }
  secret_accessor_configs = {
    data_watcher = {
      secrets_to_access = [
        { secret_key = "secure_computation_root_ca" },
        { secret_key = "data_watcher_tls_key" },
        { secret_key = "data_watcher_tls_pem" }
      ]
    },
    requisition_fetcher = {
      secrets_to_access = [
        { secret_key = "kingdom_root_ca" },
        { secret_key = "edp7_tls_key" },
        { secret_key = "edp7_tls_pem" }
      ]
    },
    event_group_sync = {
      secrets_to_access = [
        { secret_key = "kingdom_root_ca" },
        { secret_key = "edp7_tls_key" },
        { secret_key = "edp7_tls_pem" }
      ]
    }
  }
  queue_worker_configs = {
    requisition_fulfiller = {
      queue = {
        subscription_name     = "requisition-fulfiller-subscription"
        topic_name            = "requisition-fulfiller-queue"
        ack_deadline_seconds  = 600
      }
      worker = {
        instance_template_name      = "requisition-fulfiller-template"
        base_instance_name          = "secure-computation"
        managed_instance_group_name = "requisition-fulfiller-mig"
        mig_service_account_name    = "requisition-fulfiller-sa"
        single_instance_assignment  = 1
        min_replicas                = 1
        max_replicas                = 10
        app_args = [
          "--kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443",
          "--kingdom-public-api-cert-host=results-fulfiller.kingdom.dev.halo-cmm.org",
          "--secure-computation-public-api-cert-host=results-fulfiller.secure-computation.dev.halo-cmm.org",
          "--subscription-id=requisition-fulfiller-subscription",
          "--google-pub-sub-project-id=halo-cmm-dev"
        ]
        machine_type                = "n2d-standard-2"
        docker_image                = "ghcr.io/world-federation-of-advertisers/edp-aggregator/results_fulfiller:d1735f324a91d6e2ed4f395044347d95641750b9"
        secrets_to_mount            = [
          {
            secret_key              = "edpa_tee_app_tls_key"
            version                 = "latest"
            mount_path              = "/etc/ssl/edpa_tee_app_tls.key"
            flag_name               = "--edpa-tls-key-file-path"
          },
          {
            secret_key              = "edpa_tee_app_tls_pem"
            version                 = "latest"
            mount_path              = "/etc/ssl/edpa_tee_app_tls.pem"
            flag_name               = "--edpa-tls-cert-file-path"
          },
          {
            secret_key              = "secure_computation_root_ca"
            version                 = "latest"
            mount_path              = "/etc/ssl/secure_computation_root.pem"
            flag_name               = "--secure-computation-cert-collection-file-path"
          },
          {
            secret_key              = "kingdom_root_ca"
            version                 = "latest"
            mount_path              = "/etc/ssl/kingdom_root.pem"
            flag_name               = "--kingdom-cert-collection-file-path"
          },
          {
            secret_key              = "edp7_cert_der"
            version                 = "latest"
            mount_path              = "/etc/ssl/edp7_cs_cert.der"
          },
          {
            secret_key              = "edp7_private_der"
            version                 = "latest"
            mount_path              = "/etc/ssl/edp7_cs_private.der"
          },
          {
            secret_key              = "edp7_enc_private"
            version                 = "latest"
            mount_path              = "/etc/ssl/edp7_enc_private.tink"
          },
          {
            secret_key              = "edp7_tls_key"
            version                 = "latest"
            mount_path              = "/etc/ssl/edp7_tls.key"
          },
          {
            secret_key              = "edp7_tls_pem"
            version                 = "latest"
            mount_path              = "/etc/ssl/edp7_tls.pem"
          },
        ]
      }
    }
  }
}

module "edp_aggregator" {
  source = "../modules/edp-aggregator"

  key_ring_name                             = "edpa-secure-computation-cloud-test-key-ring-4"
  key_ring_location                         = local.key_ring_location
  kms_key_name                              = "edpa-secure-computation-kek"
  queue_worker_configs                      = local.queue_worker_configs
  secrets                                   = local.secrets
  secret_accessor_configs                   = local.secret_accessor_configs
  pubsub_iam_service_account_member         = module.secure_computation.secure_computation_internal_iam_service_account_member
  edp_aggregator_bucket_name                = var.secure_computation_storage_bucket_name
  edp_aggregator_bucket_location            = local.storage_bucket_location
  data_watcher_service_account_name         = "edpa-data-watcher"
  data_watcher_trigger_service_account_name = "edpa-data-watcher-trigger"
  terraform_service_account                 = var.terraform_service_account
  requisition_fetcher_service_account_name  = "edpa-requisition-fetcher"
  event_group_sync_service_account_name     = "edpa-event-group-sync"
  event_group_sync_function_name            = "event-group-sync"

}
