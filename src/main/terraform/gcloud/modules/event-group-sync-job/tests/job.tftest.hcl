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
      name  = "projects/example-project/serviceAccounts/egs@example-project.iam.gserviceaccount.com"
      email = "egs@example-project.iam.gserviceaccount.com"
    }
  }
}
variables {
  project                   = "example-project"
  region                    = "us-central1"
  name                      = "egs-job"
  function_name             = "event-group-sync"
  terraform_service_account = "terraform@example-project.iam.gserviceaccount.com"
  target = {
    schedule       = "0 2 * * *"
    data_provider  = "dataProviders/example"
    input_blob_uri = "gs://example-bucket/provider/event_groups.json"
    caller_image   = "gcr.io/example/caller@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  }
}
run "asynchronous_authenticated_start" {
  command = plan
  assert {
    condition     = google_cloud_scheduler_job.sync.http_target[0].uri == "https://run.googleapis.com/v2/projects/example-project/locations/us-central1/jobs/egs-job:run" && base64decode(google_cloud_scheduler_job.sync.http_target[0].body) == "{}"
    error_message = "Scheduler must start the execution through the Jobs API."
  }
  assert {
    condition     = google_cloud_scheduler_job.sync.http_target[0].oauth_token[0].scope == "https://www.googleapis.com/auth/cloud-platform" && google_cloud_scheduler_job.sync.retry_config[0].retry_count == 0
    error_message = "The API start uses OAuth and must not automatically start duplicate executions."
  }
  assert {
    condition     = google_cloud_run_v2_job.sync.template[0].template[0].timeout == "4200s" && google_cloud_run_v2_job.sync.template[0].template[0].max_retries == 0 && google_cloud_run_v2_job.sync.template[0].task_count == 1 && google_cloud_run_v2_job.sync.template[0].parallelism == 1
    error_message = "One task must outlive the HTTP request, without whole-sync task retries."
  }
  assert {
    condition     = google_cloud_run_service_iam_member.invoke_function.service == "event-group-sync" && google_cloud_run_v2_job_iam_member.start_job.name == "egs-job" && google_cloud_run_v2_job_iam_member.start_job.role == "roles/run.invoker"
    error_message = "Each caller must have invocation rights only on its target."
  }
  assert {
    condition     = { for env in google_cloud_run_v2_job.sync.template[0].template[0].containers[0].env : env.name => env.value }["INPUT_BLOB_URI"] == "gs://example-bucket/provider/event_groups.json"
    error_message = "The job must pass the configured object URI to EGS."
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
      caller_image   = "gcr.io/example/caller@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
  }
  assert {
    condition     = google_cloud_scheduler_job.sync.paused && google_cloud_scheduler_job.sync.schedule == "0 4 * * *" && google_cloud_scheduler_job.sync.time_zone == "Europe/London"
    error_message = "Schedule, time zone, and pause must be configurable."
  }
}
run "reject_mutable_image" {
  command = plan
  variables {
    target = {
      schedule       = "0 2 * * *"
      data_provider  = "dataProviders/example"
      input_blob_uri = "gs://example-bucket/event_groups.json"
      caller_image   = "gcr.io/example/caller:latest"
    }
  }
  expect_failures = [var.target]
}
