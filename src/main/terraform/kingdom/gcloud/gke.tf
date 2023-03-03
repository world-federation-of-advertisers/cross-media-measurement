# Copyright 2020 The Cross-Media Measurement Authors
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

# This is step 4 as per the document https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/docs/gke/kingdom-deployment.md

# Create a GKE cluster with 3 node pools
resource "google_container_cluster" "cluster" {
  name     = "kingdom-cluster"
  location = var.region

  # Configure the cluster
  initial_node_count = 1

  # Add three node pools
  node_pool {
    name = "gcp-kingdom-data-server"
    node_count = 1
    node_config {
      machine_type = "n1-standard-1"
      disk_size_gb = 100
      disk_type    = "pd-standard"
      labels = {
        app = "gcp-kingdom-data-server"
      }
    }
  }

  node_pool {
    name = "system-api-server"
    node_count = 1
    node_config {
      machine_type = "n1-standard-1"
      disk_size_gb = 100
      disk_type    = "pd-standard"
      labels = {
        app = "system-api-server"
      }
    }
  }

  node_pool {
    name = "v2alpha-public-api-server"
    node_count = 1
    node_config {
      machine_type = "n1-standard-1"
      disk_size_gb = 100
      disk_type    = "pd-standard"
      labels = {
        app = "v2alpha-public-api-server"
      }
    }
  }
}

