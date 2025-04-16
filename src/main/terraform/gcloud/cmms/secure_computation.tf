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

#TODO(@MarcoPremier): Update `configure-secure-computation-control-plane.yml` to use kingdom cluster (https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/.github/workflows/configure-secure-computation-control-plane.yml#L98)

queue_configs = {
  requisition_fulfiller = {
    instance_template_name      = "req-fulfiller-template"
    base_instance_name          = "sc"
    managed_instance_group_name = "req-fulfiller-mig"
    subscription_name           = "requisition-fulfiller-subscription"
    topic_name                  = "requisition-fulfiller-queue"
    mig_service_account_name    = "req-fulfiller-sa"
    single_instance_assignment  = 1
    min_replicas                = 1
    max_replicas                = 10
    app_args                    = []
    machine_type                = "n2d-standard-2"
    kms_key_id                  = var.kms_key_id
    docker_image                = "" # @TODO(MarcoPremier): set this value once TEE APP is merged
  }
}

module "secure_computation" {
  source = "../modules/secure-computation"

  spanner_instance                          = google_spanner_instance.spanner_instance
  data_watcher_trigger_service_account_name = "data-watcher-trigger"
  data_watcher_service_account_name         = "data-watcher"
  secure_computation_bucket_name            = "secure-computation-storage"
  secure_computation_bucket_location        = local.storage_bucket_location
  ack_deadline_seconds                      = 600
}

module "edp_aggregator" {
  source = "../modules/edp-aggregator"

  key_ring_name                             = "secure-computation-test-key-ring"
  key_ring_location                         = local.key_ring_location
  kms_key_name                              = "secure-computation-test-kek"
  queue_configs                             = queue_configs
  artifacts_registry_repo_name              = "secure-computation-tee-app"
  pubsub_iam_service_account_member         = module.secure_computation.pubsub_iam_service_account_member
}

