# Copyright 2026 The Cross-Media Measurement Authors
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

locals {
  function_uri = "https://${var.region}-${var.project}.cloudfunctions.net/${var.function_name}"
  request_body = jsonencode({
    "@type"      = "type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.EventGroupSyncParams"
    dataProvider = var.target.data_provider
  })
}

resource "google_service_account" "scheduler" {
  project      = var.project
  account_id   = var.name
  display_name = "Scheduled EventGroupSync caller"
}

resource "google_service_account_iam_member" "terraform_act_as" {
  service_account_id = google_service_account.scheduler.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}

resource "google_cloud_run_service_iam_member" "invoke" {
  project  = var.project
  location = var.region
  service  = var.function_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_cloud_scheduler_job" "sync" {
  project          = var.project
  region           = var.region
  name             = var.name
  description      = "Synchronize the current event group object"
  schedule         = var.target.schedule
  time_zone        = var.target.time_zone
  paused           = var.target.paused
  attempt_deadline = "1800s"
  retry_config {
    retry_count        = 0
    max_retry_duration = "0s"
  }
  http_target {
    http_method = "POST"
    uri         = local.function_uri
    body        = base64encode(local.request_body)
    headers = {
      "Content-Type"       = "application/json"
      "X-DataWatcher-Path" = var.target.input_blob_uri
    }
    oidc_token {
      service_account_email = google_service_account.scheduler.email
      audience              = local.function_uri
    }
  }
  depends_on = [google_cloud_run_service_iam_member.invoke, google_service_account_iam_member.terraform_act_as]
}
