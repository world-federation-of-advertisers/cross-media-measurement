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

data "google_project" "project" {}

resource "google_container_cluster" "cluster" {
  name     = var.name
  location = var.location

  workload_identity_config {
    workload_pool = "${data.google_project.project.project_id}.svc.id.goog"
  }

  database_encryption {
    key_name = var.secret_key.id
    state    = "ENCRYPTED"
  }

  datapath_provider = "ADVANCED_DATAPATH"
  networking_mode   = "VPC_NATIVE"
  ip_allocation_policy {}

  logging_config {
    enable_components = ["SYSTEM_COMPONENTS", "WORKLOADS"]
  }

  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS", "HPA", "DEPLOYMENT", "POD"]
    managed_prometheus {
      enabled = true
    }
  }

  enable_l4_ilb_subsetting = true

  release_channel {
    channel = var.release_channel
  }

  cluster_autoscaling {
    autoscaling_profile = var.autoscaling_profile
  }

  # We can't create a cluster with no node pool defined, but we want to only use
  # separately managed node pools. So we create the smallest possible default
  # node pool and immediately delete it.
  #
  # See https://registry.terraform.io/providers/hashicorp/google/4.63.0/docs/resources/container_cluster#example-usage---with-a-separately-managed-node-pool-recommended
  remove_default_node_pool = true
  initial_node_count       = 1
  node_config {
    shielded_instance_config {
      enable_secure_boot = true
    }
  }

  deletion_protection = var.deletion_protection

  lifecycle {
    ignore_changes = [
      # This only affects the default node pool, which is immediately deleted.
      node_config,
    ]
  }
}
