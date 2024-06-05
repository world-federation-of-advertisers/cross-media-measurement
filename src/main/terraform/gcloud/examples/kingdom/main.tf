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

provider "google" {}

data "google_client_config" "default" {}

locals {
  cluster_location  = var.cluster_location == null ? data.google_client_config.default.zone : var.cluster_location
  key_ring_location = var.key_ring_location == null ? data.google_client_config.default.region : var.key_ring_location
}

module "common" {
  source = "../../modules/common"

  key_ring_name     = var.key_ring_name
  key_ring_location = local.key_ring_location
}

resource "google_spanner_instance" "spanner_instance" {
  name             = var.spanner_instance_name
  config           = var.spanner_instance_config
  display_name     = "Halo CMMS"
  processing_units = 100
}

module "kingdom_cluster" {
  source = "../../modules/cluster"

  name            = var.cluster_name
  location        = local.cluster_location
  release_channel = var.cluster_release_channel
  secret_key      = module.common.cluster_secret_key
}

module "kingdom_default_node_pool" {
  source = "../../modules/node-pool"

  name            = "default"
  cluster         = module.kingdom_cluster.cluster
  service_account = module.common.cluster_service_account
  machine_type    = "e2-custom-2-4096"
  max_node_count  = 2
}

module "kingdom" {
  source = "../../modules/kingdom"

  spanner_instance = google_spanner_instance.spanner_instance
}
