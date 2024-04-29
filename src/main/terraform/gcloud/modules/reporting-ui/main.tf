# Copyright 2024 The Cross-Media Measurement Authors
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

module "reporting_ui_user" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "reporting-ui-server"
  iam_service_account_name        = var.iam_service_account_name_ui
  iam_service_account_description = "Reporting UI server."
}

module "reporting_gateway_user" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "reporting-gateway-server"
  iam_service_account_name        = var.iam_service_account_name_gateway
  iam_service_account_description = "Reporting Gateway server."
}

module "reporting_grpc_user" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "reporting-grpc-server"
  iam_service_account_name        = var.iam_service_account_name_grpc
  iam_service_account_description = "Reporting GRPC server."
}

resource "google_storage_bucket" "ui_server_bucket" {
  name          = "reporting_ui"
  location      = "US-CENTRAL1"
  force_destroy = false
  uniform_bucket_level_access = true
}

resource "google_storage_bucket_iam_member" "ui_server_bucket_member" {
  bucket = google_storage_bucket.ui_server_bucket.name
  role   = "roles/storage.objectViewer"
  member = module.reporting_ui_user.iam_service_account.member
}
