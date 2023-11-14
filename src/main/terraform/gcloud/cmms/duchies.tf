# Copyright 2023 The Cross-Media Measurement Authors
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

module "clusters" {
  source   = "../modules/cluster"
  for_each = local.duchy_names

  name       = "${each.key}-duchy"
  location   = local.cluster_location
  secret_key = module.common.cluster_secret_key
}

data "google_container_cluster" "clusters" {
  for_each = local.duchy_names

  name     = "${each.key}-duchy"
  location = local.cluster_location

  # Defer reading of cluster resource until it exists.
  depends_on = [module.clusters]
}

module "default_node_pools" {
  source   = "../modules/node-pool"
  for_each = data.google_container_cluster.clusters

  cluster         = each.value
  name            = "default"
  service_account = module.common.cluster_service_account
  machine_type    = "e2-standard-2"
  max_node_count  = 2
}

module "highmem_node_pools" {
  source   = "../modules/node-pool"
  for_each = data.google_container_cluster.clusters

  cluster         = each.value
  name            = "highmem"
  service_account = module.common.cluster_service_account
  machine_type    = "c3-highcpu-4"
  max_node_count  = 2
  spot            = true
}

module "storage" {
  source = "../modules/storage-bucket"

  name     = var.storage_bucket_name
  location = local.storage_bucket_location
}

# TODO(hashicorp/terraform#24476): Use a for_each for the Duchy modules once
# that works with providers.

module "aggregator_duchy" {
  source = "../modules/duchy"

  name             = "aggregator"
  database_name    = "aggregator_duchy_computations"
  spanner_instance = google_spanner_instance.spanner_instance
  storage_bucket   = module.storage.storage_bucket
}

module "worker1_duchy" {
  source = "../modules/duchy"

  name             = "worker1"
  database_name    = "worker1_duchy_computations"
  spanner_instance = google_spanner_instance.spanner_instance
  storage_bucket   = module.storage.storage_bucket
}

module "worker2_duchy" {
  source = "../modules/duchy"

  name             = "worker2"
  database_name    = "worker2_duchy_computations"
  spanner_instance = google_spanner_instance.spanner_instance
  storage_bucket   = module.storage.storage_bucket
}
