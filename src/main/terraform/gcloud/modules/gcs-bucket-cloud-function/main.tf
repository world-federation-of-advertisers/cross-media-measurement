# Copyright 2025 The Cross-Media Measurement Authors
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

resource "google_service_account" "cloud_function_service_account" {
  account_id   = var.cloud_function_service_account_name
  display_name = "Service account for Cloud Function"
}

resource "google_service_account" "cloud_function_trigger_service_account" {
  account_id   = var.cloud_function_trigger_service_account_name
  display_name = "Trigger Service Account for Cloud Function"
}

resource "google_service_account_iam_member" "allow_terraform_to_use_cloud_function_service_account" {
  service_account_id = google_service_account.cloud_function_service_account.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}

resource "google_storage_bucket_iam_member" "cloud_function_object_viewer" {
  bucket = var.trigger_bucket_name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.cloud_function_service_account.email}"
}

resource "google_storage_bucket_iam_member" "cloud_function_object_creator" {
  bucket = var.trigger_bucket_name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.cloud_function_service_account.email}"
}

resource "google_project_iam_member" "trigger_event_receiver" {
  project = data.google_project.project.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.cloud_function_trigger_service_account.email}"
}

resource "google_project_iam_member" "trigger_run_invoker" {
  project = data.google_project.project.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.cloud_function_trigger_service_account.email}"
}
