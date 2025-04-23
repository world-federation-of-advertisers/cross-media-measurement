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

resource "google_service_account" "http_cloud_function_service_account" {
  account_id   = var.http_cloud_function_service_account_name
  display_name = "Service account for Cloud Function"
}

resource "google_storage_bucket_iam_member" "http_cloud_function_object_viewer" {
  bucket = var.bucket_name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.http_cloud_function_service_account.email}"
}

resource "google_storage_bucket_iam_member" "http_cloud_function_object_creator" {
  bucket = var.bucket_name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.http_cloud_function_service_account.email}"
}
