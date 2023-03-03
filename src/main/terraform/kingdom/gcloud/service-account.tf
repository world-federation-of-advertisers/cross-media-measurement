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

# Create service account for accessing Cloud Spanner
resource "google_service_account" "spanner_service_account" {
account_id   = "spanner-access-sa"
display_name = "Spanner Access Service Account"
}

# Grant Cloud Spanner database access to the service account
resource "google_project_iam_member" "spanner_access" {
project = var.project_id
role    = "roles/spanner.databaseUser"
member  = "serviceAccount:${google_service_account.spanner_service_account.email}"
}

# Create GKE service account for workload identity
resource "google_service_account" "gke_sa" {
account_id   = "gke-cluster-sa"
display_name = "GKE Service Account"
}

# Bind IAM role to GKE service account
resource "google_project_iam_binding" "gke_sa_iam_binding" {
project = var.project_id
role    = "roles/iam.workloadIdentityUser"

members = [
"serviceAccount:${google_service_account.gke_sa.email}"
]
}

# Create Kubernetes service account
resource "kubernetes_service_account" "internal-server" {
metadata {
name = "internal-server"
}
}

# Grant Cloud Spanner database access to GKE service account
resource "google_spanner_database_iam_binding" "database_iam_binding" {
  project    = "halo-cmm-sandbox"
  instance   = google_spanner_instance.main.name
  database   = google_spanner_database.database.name
  role       = "roles/spanner.databaseUser"
  members    = [ "serviceAccount:${google_service_account.spanner_service_account.email}" ]
}
