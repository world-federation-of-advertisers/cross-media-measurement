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

# This is step 3 and 5 as per the document
# https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/docs/gke/kingdom-deployment.md

# Create service account for accessing Cloud Spanner
# Grant Cloud Spanner database access to the service account for the considered Project ID
# Bind service-account <------> project_id <----------> spanner_db

data "google_service_account" "spanner_service_account"{
  account_id = "spanner-access-sa"
}


# Create GKE service account for workload identity
# Bind IAM role to GKE service account

data "google_service_account" "gke_sa" {
  account_id = "gke-cluster-sa"
}

resource "google_project_iam_binding" "gke_sa_iam_binding" {
  project = local.project
  role    = "roles/iam.workloadIdentityUser"
  members = [
    "serviceAccount:${data.google_service_account.gke_sa.email}"
  ]
}

# Create the service account for worker1 Duchy DB
resource "google_service_account" "worker1_db" {
  account_id   = "worker1-db-sa"
  display_name = "Worker1 Duchy DB Service Account"
}

# Create the IAM policy binding for worker1 Duchy DB service account
resource "google_project_iam_binding" "worker1_db_iam_binding" {
  project = local.project

  role    = "roles/cloudsql.client"
  members = [
    "serviceAccount:${google_service_account.worker1_db.email}"
  ]
}

# Create the service account for worker2 Duchy DB
resource "google_service_account" "worker2_db" {
  account_id   = "worker2-db-sa"
  display_name = "Worker2 Duchy DB Service Account"
}

# Create the IAM policy binding for worker2 Duchy DB service account
resource "google_project_iam_binding" "worker2_db_iam_binding" {
  project = local.project

  role    = "roles/cloudsql.client"
  members = [
    "serviceAccount:${google_service_account.worker2_db.email}"
  ]
}

# Create the service account for aggregator Duchy DB
resource "google_service_account" "aggregator_db" {
  account_id   = "aggregator-db-sa"
  display_name = "Aggregator Duchy DB Service Account"
}

# Create the IAM policy binding for aggregator Duchy DB service account
resource "google_project_iam_binding" "aggregator_db_iam_binding" {
  project = local.project

  role    = "roles/cloudsql.client"
  members = [
    "serviceAccount:${google_service_account.aggregator_db.email}"
  ]
}

# Create the service account for GCS bucket access
resource "google_service_account" "gcs_access" {
  account_id   = "gcs-access-sa"
  display_name = "GCS Bucket Access Service Account"
}

# Create the IAM policy binding for GCS bucket access service account
resource "google_storage_bucket_iam_binding" "gcs_access_iam_binding" {
  bucket = "${local.prefix}-storage"

  role    = "roles/storage.objectViewer"
  members = [
    "serviceAccount:${google_service_account.gcs_access.email}"
  ]
}
