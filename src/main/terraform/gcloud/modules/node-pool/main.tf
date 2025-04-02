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


resource "google_container_node_pool" "node_pool" {
  name    = var.name
  cluster = var.cluster.id

  node_config {
    service_account = var.service_account.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    machine_type    = var.machine_type
    disk_type       = "pd-balanced"
    spot            = var.spot
    shielded_instance_config {
      enable_secure_boot = true
    }
    kubelet_config {
      cpu_manager_policy                     = "none"
      insecure_kubelet_readonly_port_enabled = "FALSE"
    }

    dynamic "taint" {
      for_each = var.spot ? [{
        key    = "cloud.google.com/gke-spot"
        value  = "true"
        effect = "NO_SCHEDULE"
      }] : []

      content {
        key    = taint.value["key"]
        value  = taint.value["value"]
        effect = taint.value["effect"]
      }
    }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }

  autoscaling {
    location_policy = "BALANCED"
    min_node_count  = 0
    max_node_count  = var.max_node_count
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
  }
}
