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
  queues_config = {
    subscription_name     = "requisition-fulfiller-subscription"
    topic_name            = "requisition-fulfiller-queue"
    ack_deadline_seconds  = 600
  }

  workers_config = {
    requisition_fulfiller = {
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
    }
  }
}


module "edp_aggregator" {
  source = "../modules/edp-aggregator"

  key_ring_name                             = "secure-computation-test-key-ring"
  key_ring_location                         = local.key_ring_location
  kms_key_name                              = "secure-computation-test-kek"
  queues_config                             = local.queues_config
  workers_config                            = local.workers_config
  artifacts_registry_repo_name              = "secure-computation-tee-app"
  pubsub_iam_service_account_member         = module.secure_computation.secure_computation_internal_iam_service_account_member
  edp_aggregator_bucket_name                = "secure-computation-storage"
  edp_aggregator_bucket_location            = local.storage_bucket_location
  data_watcher_service_account_name         = "data-watcher"
  data_watcher_trigger_service_account_name = "data-watcher-trigger"
}

