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

resource "google_service_account" "scheduler_service_account" {
  account_id   = var.scheduler_service_account_name
  display_name = "Service account for Cloud Scheduler"
}

resource "google_cloud_scheduler_job" "job" {
  name             = var.job_name
  description      = var.job_description
  schedule         = var.schedule
  time_zone        = var.time_zone
  attempt_deadline = var.attempt_deadline

  http_target {
    http_method = "POST"
    uri         = var.function_uri

    oidc_token {
      service_account_email = google_service_account.scheduler_service_account.email
    }
  }
}

resource "google_service_account_iam_member" "allow_terraform_to_use_scheduler_service_account" {
  service_account_id = google_service_account.scheduler_service_account.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}

# Grant the scheduler service account permission to invoke the Cloud Function
resource "google_cloudfunctions_function_iam_member" "invoker" {
  project        = data.google_project.project.project_id
  region         = var.region
  cloud_function = var.cloud_function_name
  role           = "roles/cloudfunctions.invoker"
  member         = "serviceAccount:${google_service_account.scheduler_service_account.email}"
}