# Copyright 2023 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This is step 4 as per the document
# https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/docs/gke/kingdom-deployment.md

resource "google_container_cluster" "primary" {

  # the name will look like dev-halo-duchy-gke-cluster
  name     = local.kingdom.name
  location = "us-central1-a" # TODO(wfa-dharmalingam): change it to local.zone
  initial_node_count = local.kingdom.cluster_node_count
  database_encryption {
    key_name = local.kingdom.encryption_key
    state = local.kingdom.database_encryption_state
  }
  cluster_autoscaling {
    enabled = local.kingdom.auto_scaling
    dynamic "auto_provisioning_defaults" {
      for_each = local.kingdom.auto_scaling ? [1] : []

      content {
        service_account = google_service_account.gke_sa.email
        oauth_scopes = [
          "https://www.googleapis.com/auth/cloud-platform"
        ]
      }
    }
    dynamic "resource_limits" {
      for_each = local.autoscaling_resource_limits
      content {
        resource_type = lookup(resource_limits.value, "resource_type")
        minimum       = lookup(resource_limits.value, "minimum")
        maximum       = lookup(resource_limits.value, "maximum")
      }
    }
  }
}

resource "google_container_node_pool" "kingdom_node_pool"{

  # the name will look like dev-halo-duchy-data-server
  name       = "${local.prefix}-node-pool"
  cluster    = google_container_cluster.primary.id
  location = "us-central1-a" # TODO(wfa-dharmalingam): change it to local.zone

  autoscaling {
    max_node_count = local.kingdom.max_node_count
    min_node_count = local.kingdom.min_node_count
  }

  node_config {
    preemptible  = true
    machine_type = local.kingdom.machine_type
    service_account = google_service_account.gke_sa.email
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }
}
