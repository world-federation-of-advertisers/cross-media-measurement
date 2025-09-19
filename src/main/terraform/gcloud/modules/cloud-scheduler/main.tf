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

data "google_client_config" "default" {}

resource "google_service_account" "scheduler_service_account" {
  account_id   = var.scheduler_config.name
  display_name = var.scheduler_config.scheduler_sa_display_name
  description  = var.scheduler_config.scheduler_sa_description
}

resource "google_service_account_iam_member" "allow_terraform_to_use_scheduler_service_account" {
  service_account_id = google_service_account.scheduler_service_account.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}

resource "google_project_iam_member" "scheduler_function_invoker" {
  project = data.google_client_config.default.project
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.scheduler_service_account.email}"
}

resource "google_cloud_scheduler_job" "scheduler_job" {
  name        = "${var.scheduler_config.name}-requisition-fetcher"
  description = var.scheduler_config.scheduler_job_description
  schedule    = var.scheduler_config.schedule
  time_zone   = var.scheduler_config.time_zone

  http_target {
    http_method = "POST"
    uri         = var.scheduler_config.function_url

    headers = {
      "Content-Type" = "application/json"
    }

    oidc_token {
      service_account_email = google_service_account.scheduler_service_account.email
    }
  }

  depends_on = [
      google_service_account_iam_member.allow_terraform_to_use_scheduler_service_account,
      google_project_iam_member.scheduler_function_invoker
    ]
}