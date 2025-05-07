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
        app_args                    = []
        machine_type                = "n2d-standard-2"
        docker_image                = "" # @TODO(MarcoPremier): set this value once TEE APP is merged
        secrets_to_mount            = [
          {
            secret_id               = "edpa-tee-app-tls-key"
            version                 = "latest"
            mount_path              = "/etc/ssl/edpa_tee_app_tls.key"
            secret_local_path       = "${path.root}/../../../k8s/testing/secretfiles/edpa_tee_app_tls.key"
            flag_name               = "--edpa-tls-key-file-path"
          },
          {
            secret_id               = "edpa-tee-app-tls-pem"
            version                 = "latest"
            mount_path              = "/etc/ssl/edpa_tee_app_tls.pem"
            secret_local_path       = "${path.root}/../../../k8s/testing/secretfiles/edpa_tee_app_tls.pem"
            flag_name               = "--edpa-tls-cert-file-path"
          },
          {
            secret_id               = "secure-computation-root-ca"
            version                 = "latest"
            mount_path              = "/etc/ssl/secure_computation_root.pem"
            secret_local_path       = "${path.root}/../../../k8s/testing/secretfiles/secure_computation_root.pem"
            flag_name               = "--secure-computation-cert-collection-file-path"
          },
          {
            secret_id               = "kingdom-root-ca"
            version                 = "latest"
            mount_path              = "/etc/ssl/kingdom_root.pem"
            secret_local_path       = "${path.root}/../../../k8s/testing/secretfiles/kingdom_root.pem"
            flag_name               = "--kingdom-cert-collection-file-path"
          },
          {
            secret_id               = "edp7-result-cert-der"
            version                 = "latest"
            mount_path              = "/etc/ssl/edp7_result_cs_cert.der"
            secret_local_path       = "${path.root}/../../../k8s/testing/secretfiles/edp7_result_cs_cert.der"
          },
          {
            secret_id               = "edp7-result-private-der"
            version                 = "latest"
            mount_path              = "/etc/ssl/edp7_result_cs_private.der"
            secret_local_path       = "${path.root}/../../../k8s/testing/secretfiles/edp7_result_cs_private.der"
          },
          {
            secret_id               = "edp7-enc-private"
            version                 = "latest"
            mount_path              = "/etc/ssl/edp7_enc_private.tink"
            secret_local_path       = "${path.root}/../../../k8s/testing/secretfiles/edp7_enc_private.tink"
          },
        ]
      }
    }
  }
}

module "edp_aggregator" {
  source = "../modules/edp-aggregator"

  key_ring_name                             = "secure-computation-cloud-test-key-ring-3"
  key_ring_location                         = local.key_ring_location
  kms_key_name                              = "secure-computation-kek"
  queue_worker_configs                      = local.queue_worker_configs
  pubsub_iam_service_account_member         = module.secure_computation.secure_computation_internal_iam_service_account_member
  edp_aggregator_bucket_name                = var.secure_computation_storage_bucket_name
  edp_aggregator_bucket_location            = local.storage_bucket_location
  data_watcher_service_account_name         = "data-watcher"
  data_watcher_trigger_service_account_name = "data-watcher-trigger"
  terraform_service_account                 = var.terraform_service_account
}

