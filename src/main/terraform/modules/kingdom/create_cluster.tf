# Copyright 2020 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

resource "google_container_cluster" "primary_cluster" {
  name     = var.cluster_info.primary_name
  location = data.google_client_config.current.zone
  remove_default_node_pool = true
  initial_node_count       = 1

  network_policy {
    provider = "CALICO"
    enabled = true
  }

  database_encryption {
    state = "ENCRYPTED"
    key_name = "projects/${data.google_client_config.current.project}/locations/${data.google_client_config.current.region}/keyRings/${var.kms_data.key_ring_name}/cryptoKeys/${var.kms_data.key_id}"
  }

  workload_identity_config {
    workload_pool = "${data.google_client_config.current.project}.svc.id.goog"
  }
}

resource "google_container_node_pool" "primary_preemptible_nodes" {
  location   = data.google_client_config.current.zone
  cluster    = google_container_cluster.primary_cluster.name
  node_count = 3
  autoscaling {
    min_node_count = 1
    max_node_count = 5
  }

  node_config {
    machine_type = "e2-small"

    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    service_account = "${google_service_account.cluster_service_account.email}"
    oauth_scopes    = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }
}
