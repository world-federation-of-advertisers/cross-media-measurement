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
  raw_secrets = jsondecode(var.edpa_secrets)

  secrets = {
    for key, value in local.raw_secrets : key => {
      secret_id         = value.secret_id
      secret_local_path = abspath("${path.root}/${value.secret_local_path}")
      is_binary_format  = value.is_binary_format
    }
  }
  secret_accessor_configs = jsondecode(var.edpa_secret_accessor_configs)
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
          "--secure-computation-public-api-target=v1alpha.secure-computation.dev.halo-cmm.org:8443",
          "--kingdom-public-api-cert-host=results-fulfiller.kingdom.dev.halo-cmm.org",
          "--secure-computation-public-api-cert-host=results-fulfiller.secure-computation.dev.halo-cmm.org",
          "--subscription-id=requisition-fulfiller-subscription",
          "--google-pub-sub-project-id=halo-cmm-dev"
        ]
        machine_type                = "n2d-standard-2"
        docker_image                = "ghcr.io/world-federation-of-advertisers/edp-aggregator/results_fulfiller:a25ce11fa004967647cf6b2bdb237e8fa9a24849"
        secrets_to_mount            = jsondecode(var.requisition_fulfiller_secrets_to_mount)
      }
    }
  }
}

module "edp_aggregator" {
  source = "../modules/edp-aggregator"

  key_ring_name                             = "edpa-secure-computation-cloud-test-key-ring-9"
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
