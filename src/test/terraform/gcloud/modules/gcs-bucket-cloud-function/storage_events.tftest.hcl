# Copyright 2026 The Cross-Media Measurement Authors
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

mock_provider "google" {
  mock_data "google_project" {
    defaults = { id = "test-project", number = "123456" }
  }
  mock_data "google_client_config" {
    defaults = { region = "us-central1" }
  }
}

variables {
  cloud_function_service_account_name         = "cleanup-worker"
  cloud_function_trigger_service_account_name = "cleanup-trigger"
  trigger_bucket_name                         = "test-impressions"
  terraform_service_account                   = "terraform@test-project.iam.gserviceaccount.com"
  function_name                               = "data-watcher-delete"
  entry_point                                 = "org.example.DataWatcherFunction"
  extra_env_vars                              = ""
  secret_mappings                             = ""
  uber_jar_path                               = "/tmp/test-function/function.jar"
  trigger_event_type                          = "deleted"
}

run "default_keeps_one_storage_event" {
  command = plan

  assert {
    condition     = length(google_eventarc_trigger.additional_storage_event) == 0
    error_message = "Functions must retain their existing single-event behavior by default."
  }
}

run "archive_events_reach_existing_cleanup_function" {
  command = plan

  variables {
    additional_trigger_event_types = ["archived"]
  }

  assert {
    condition = {
      for criterion in google_eventarc_trigger.additional_storage_event["archived"].matching_criteria :
      criterion.attribute => criterion.value
      } == {
      type   = "google.cloud.storage.object.v1.archived"
      bucket = "test-impressions"
    }
    error_message = "Archive notifications must come from the configured impression bucket."
  }

  assert {
    condition = (
      google_eventarc_trigger.additional_storage_event["archived"].destination[0].cloud_run_service[0].service == "data-watcher-delete" &&
      google_eventarc_trigger.additional_storage_event["archived"].destination[0].cloud_run_service[0].region == "us-central1"
    )
    error_message = "Archive notifications must reach the existing cleanup function."
  }

  assert {
    condition     = length(terraform_data.attach_additional_dead_letter_policy) == 1
    error_message = "Archive delivery failures must retain the configured dead-letter policy."
  }
}
