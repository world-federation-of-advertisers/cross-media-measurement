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

module "control_plane_cluster" {
  source = "../modules/cluster"

  deletion_protection = false

  name            = local.control_plane_cluster_name
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

module "secure_computation" {
  source = "../modules/secure-computation"
}

module "data_watcher" {
    source = "../modules/gcs-bucket-cloud-function"

  cloud_function_source_object          = var.data_watcher_source_object
  cloud_function_source_bucket          = var.cloud_function_source_bucket
  cloud_function_name                   = var.data_watcher_cloud_function_name
  entry_point                           = var.data_watcher_entry_point
  trigger_bucket_name                   = var.data_watcher_trigger_bucket_name
  cloud_function_service_account_name   = var.data_watcher_cloud_function_service_account_name
}