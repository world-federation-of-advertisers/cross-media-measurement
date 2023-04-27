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

data "google_container_cluster" "cluster" {
  name = var.cluster_name
}

resource "google_service_account" "kingdom_internal" {
  account_id  = "kingdom-internal"
  description = "Kingdom internal API server."
}

resource "google_spanner_database" "kingdom" {
  instance         = var.spanner_instance.name
  name             = "kingdom"
  database_dialect = "GOOGLE_STANDARD_SQL"
}

resource "google_spanner_database_iam_member" "kingdom_internal" {
  instance = google_spanner_database.kingdom.instance
  database = google_spanner_database.kingdom.name
  role     = "roles/spanner.databaseUser"
  member   = google_service_account.kingdom_internal.member
}

resource "kubernetes_service_account" "internal_server" {
  metadata {
    name = "internal-server"
    annotations = {
      "iam.gke.io/gcp-service-account" : google_service_account.kingdom_internal.email
    }
  }
}

resource "google_service_account_iam_member" "kingdom_internal_binding" {
  service_account_id = google_service_account.kingdom_internal.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${data.google_container_cluster.cluster.workload_identity_config[0].workload_pool}[${kubernetes_service_account.internal_server.metadata[0].namespace}/${kubernetes_service_account.internal_server.metadata[0].name}]"
}

# TODO(@SanjayVas): Add external IPs?
