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

module "cloud_function_service_account" {
  source                        = "../workload-identity-user"
  k8s_service_account_name      = "cloud_function-service-account"
  iam_service_account_name      = var.cloud_function_service_account_name
  iam_service_account_description = "Service account for Managed Instance Group"
}

resource "google_storage_bucket_iam_member" "storage_access" {
  project = data.google_project.project.name
  role    = "roles/eventarc.eventReceiver"
  member  = module.cloud_function_service_account.iam_service_account.member
}

resource "google_cloudfunctions2_function" "cloud_function" {
  name        = var.cloud_function_name
  runtime     = var.runtime
  entry_point = var.entry_point

  docker_registry  = var.docker_registry
  docker_repository = var.docker_repository

  event_trigger {
    event_type = "google.cloud.storage.object.v1.finalized"
    retry_policy = "RETRY_POLICY_RETRY"
    service_account_email = module.cloud_function_service_account.iam_service_account
    event_filters {
      attribute = "bucket"
      value = var.trigger_bucket_name
    }
  }
}