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

resource "google_service_account" "cloud_function_service_account" {
  account_id   = var.cloud_function_service_account_name
  description  = "Service account for Cloud Functions."
  display_name = "Cloud Function Service Account"
}

resource "google_storage_bucket_iam_member" "storage_event_viewer" {
  bucket = var.trigger_bucket_name
  role    = "roles/eventarc.eventReceiver"
  member = "serviceAccount:${google_service_account.cloud_function_service_account.email}"
}

resource "google_cloudfunctions2_function" "cloud_function" {
  name        = var.cloud_function_name
  entry_point = var.entry_point

  docker_registry  = var.docker_registry
  docker_repository = var.docker_repository

  event_trigger {
    event_type = "google.cloud.storage.object.v1.finalized"
    retry_policy = "RETRY_POLICY_RETRY"
    service_account_email = google_service_account.cloud_function_service_account.email
    event_filters {
      attribute = "bucket"
      value = var.trigger_bucket_name
    }
  }
}