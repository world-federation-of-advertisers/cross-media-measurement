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


# This is step number 5 as per document
# https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/docs/gke/duchy-deployment.md

resource "google_container_cluster" "worker" {

  # The name will look like dev-halo-duchy-worker
  name     = "${local.prefix}-worker"

  location = local.zone
  initial_node_count = local.duchy.cluster_node_count
  database_encryption {
    key_name = "projects/${local.project}/locations/${local.zone}/keyRings/test-key-ring/cryptoKeys/k8s-secret"
    state = "ENCRYPTED"
  }
  cluster_autoscaling {
    enabled = true
  }
}

resource "google_container_node_pool" "data_server"{

  # The name will look like dev-halo-duchy-data-server
  name       = "${local.prefix}-data-server"
  cluster    = google_container_cluster.worker.id

  autoscaling {
    max_node_count = local.duchy.max_node_count
    min_node_count = local.duchy.min_node_count
  }

  node_config {
    preemptible  = true
    machine_type = local.duchy.machine_type
    service_account = google_service_account.gke_sa.email
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }
}
