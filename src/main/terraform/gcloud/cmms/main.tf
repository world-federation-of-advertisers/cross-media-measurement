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

terraform {
  backend "gcs" {
    prefix = "terraform/state/halo-cmms"
  }
}

locals {
  kingdom_cluster_name = "kingdom"
}

provider "google" {}

data "google_client_config" "default" {}

module "common" {
  source = "../modules/common"

  key_ring_name     = var.key_ring_name
  key_ring_location = var.key_ring_location
}

resource "google_spanner_instance" "spanner_instance" {
  name             = var.spanner_instance_name
  config           = var.spanner_instance_config
  display_name     = "Halo CMMS"
  processing_units = var.spanner_processing_units
}

# TODO(@SanjayVas): Add Duchies and EDP simulators.
