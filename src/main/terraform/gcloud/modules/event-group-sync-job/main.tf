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
  # Pass request data as environment variables, never as shell program text.
  caller_script = <<-SCRIPT
    set -eu
    token=$(curl --fail --silent --show-error --max-time 30 \
      --header 'Metadata-Flavor: Google' --get \
      --data-urlencode "audience=$EGS_URL" \
      'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity')
    status=$(curl --fail --silent --show-error --connect-timeout 30 --max-time 3900 \
      --output /dev/null --write-out '%%{http_code}' \
      --request POST --header "Authorization: Bearer $token" \
      --header 'Content-Type: application/json' \
      --header "X-DataWatcher-Path: $INPUT_BLOB_URI" \
      --data "$REQUEST_BODY" "$EGS_URL")
    if [ "$status" != 200 ]; then
      echo "EventGroupSync returned HTTP $status" >&2
      exit 1
    fi
  SCRIPT
}

resource "google_service_account" "caller" {
  project      = var.project
  account_id   = "${var.name}-caller"
  display_name = "EventGroupSync job caller"
}

resource "google_service_account" "scheduler" {
  project      = var.project
  account_id   = var.name
  display_name = "EventGroupSync job scheduler"
}

resource "google_service_account_iam_member" "terraform_act_as" {
  for_each = {
    caller    = google_service_account.caller.name
    scheduler = google_service_account.scheduler.name
  }
  service_account_id = each.value
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}

resource "google_cloud_run_service_iam_member" "invoke_function" {
  project  = var.project
  location = var.region
  service  = var.function_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.caller.email}"
}

resource "google_cloud_run_v2_job" "sync" {
  project             = var.project
  location            = var.region
  name                = var.name
  deletion_protection = false
  template {
    task_count  = 1
    parallelism = 1
    template {
      service_account = google_service_account.caller.email
      timeout         = "4200s"
      max_retries     = 0
      containers {
        image   = var.target.caller_image
        command = ["/bin/sh"]
        args    = ["-c", local.caller_script]
        env {
          name  = "EGS_URL"
          value = local.function_uri
        }
        env {
          name  = "INPUT_BLOB_URI"
          value = var.target.input_blob_uri
        }
        env {
          name  = "REQUEST_BODY"
          value = local.request_body
        }
      }
    }
  }
  depends_on = [google_cloud_run_service_iam_member.invoke_function, google_service_account_iam_member.terraform_act_as]
}

resource "google_cloud_run_v2_job_iam_member" "start_job" {
  project  = var.project
  location = var.region
  name     = google_cloud_run_v2_job.sync.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_cloud_scheduler_job" "sync" {
  project          = var.project
  region           = var.region
  name             = var.name
  description      = "Start an EventGroupSync caller job"
  schedule         = var.target.schedule
  time_zone        = var.target.time_zone
  paused           = var.target.paused
  attempt_deadline = "180s"
  retry_config {
    retry_count        = 0
    max_retry_duration = "0s"
  }
  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project}/locations/${var.region}/jobs/${google_cloud_run_v2_job.sync.name}:run"
    headers     = { "Content-Type" = "application/json" }
    body        = base64encode("{}")
    oauth_token {
      service_account_email = google_service_account.scheduler.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
  depends_on = [google_cloud_run_v2_job_iam_member.start_job]
}
