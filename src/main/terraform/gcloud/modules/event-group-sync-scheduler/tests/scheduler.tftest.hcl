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

mock_provider "google" {
  mock_resource "google_service_account" {
    defaults = {
      name  = "projects/example-project/serviceAccounts/egs-schedule@example-project.iam.gserviceaccount.com"
      email = "egs-schedule@example-project.iam.gserviceaccount.com"
    }
  }
}

variables {
  project                   = "example-project"
  region                    = "us-central1"
  name                      = "egs-schedule"
  function_name             = "event-group-sync"
  terraform_service_account = "terraform@example-project.iam.gserviceaccount.com"
  target = {
    schedule       = "0 2 * * *"
    data_provider  = "dataProviders/example"
    input_blob_uri = "gs://example-bucket/provider/event_groups.json"
  }
}

run "authenticated_request" {
  command = plan
  assert {
    condition     = google_cloud_scheduler_job.sync.attempt_deadline == "1800s" && google_cloud_scheduler_job.sync.retry_config[0].retry_count == 0
    error_message = "Direct sync must use the 30 minute deadline without automatic whole-sync retries."
  }
  assert {
    condition     = google_cloud_scheduler_job.sync.http_target[0].oidc_token[0].audience == google_cloud_scheduler_job.sync.http_target[0].uri
    error_message = "The identity token audience must match the function URL."
  }
  assert {
    condition     = jsondecode(base64decode(google_cloud_scheduler_job.sync.http_target[0].body)).dataProvider == "dataProviders/example" && google_cloud_scheduler_job.sync.http_target[0].headers["X-DataWatcher-Path"] == "gs://example-bucket/provider/event_groups.json"
    error_message = "Scheduler must send the existing EGS HTTP request contract."
  }
  assert {
    condition     = google_cloud_run_service_iam_member.invoke.service == "event-group-sync" && google_cloud_run_service_iam_member.invoke.role == "roles/run.invoker"
    error_message = "Invocation permission must be scoped to EGS."
  }
}

run "paused_custom_schedule" {
  command = plan
  variables {
    target = {
      schedule       = "0 4 * * *"
      time_zone      = "Europe/London"
      paused         = true
      data_provider  = "dataProviders/example"
      input_blob_uri = "gs://example-bucket/event_groups.json"
    }
  }
  assert {
    condition     = google_cloud_scheduler_job.sync.paused && google_cloud_scheduler_job.sync.schedule == "0 4 * * *" && google_cloud_scheduler_job.sync.time_zone == "Europe/London"
    error_message = "Schedule, time zone, and pause must remain configurable."
  }
}

run "reject_invalid_object" {
  command = plan
  variables {
    target = {
      schedule       = "0 2 * * *"
      data_provider  = "dataProviders/example"
      input_blob_uri = "https://example.invalid/event_groups.json"
    }
  }
  expect_failures = [var.target]
}
