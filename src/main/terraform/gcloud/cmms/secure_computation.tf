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

module "secure_computation_cluster" {
  source = "../modules/cluster"

  deletion_protection = false

  name            = local.secure_computation_cluster_name
  location        = local.cluster_location
  release_channel = var.cluster_release_channel
  secret_key      = module.common.cluster_secret_key
}

module "control_plane_default_node_pool" {
  source = "../modules/node-pool"

  name            = "default"
  cluster         = module.control_plane_cluster.cluster
  service_account = module.common.cluster_service_account
  machine_type    = "e2-custom-2-4096"
  max_node_count  = 4
}

mig_configs = {
  requisition_fulfiller = {
    instance_template_name      = "req-fulfiller-template"
    base_instance_name          = "sc"
    managed_instance_group_name = "req-fulfiller-mig"
    subscription_id             = "sub-requisition"
    mig_service_account_name    = "req-fulfiller-sa"
    single_instance_assignment  = 1
    min_replicas                = 1
    max_replicas                = 10
    app_args                    = []
    machine_type                = "n2d-standard-2"
    kms_key_id                  = var.kms_key_id
  }
}

module "secure_computation" {
  source = "../modules/secure-computation"

  spanner_instance                          = google_spanner_instance.spanner_instance

  data_watcher_trigger_service_account_name = "data-watcher-invoker"
  data_watcher_service_account_name         = "data-watcher"
  secure_computation_bucket_name            = "secure-computation-storage"
  secure_computation_bucket_location        = local.secure_computation_bucket_location

  requisition_fulfiller_topic_name          = "requisition-fulfiller-queue"
  requisition_fulfiller_subscription_name   = "requisition-fulfiller-subscription"
  ack_deadline_seconds                      = 600

  key_ring_name                             = "secure-computation-test-key-ring"
  key_ring_location                         = local.key_ring_location
  kms_key_name                              = "secure-computation-test-kek"

  mig_names                                 = mig_configs
}

