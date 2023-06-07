# Copyright 2023 The Cross-Media Measurement Authors
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

module "simulator_user" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "simulator"
  iam_service_account_name        = "simulator"
  iam_service_account_description = "EDP simulator"
}

resource "google_storage_bucket_iam_member" "object_admin" {
  bucket = var.storage_bucket.name
  role   = "roles/storage.objectAdmin"
  member = module.simulator_user.iam_service_account.member
}

resource "google_project_iam_member" "bigquery_user" {
  project = data.google_project.project.name
  role    = "roles/bigquery.jobUser"
  member  = module.simulator_user.iam_service_account.member
}

resource "google_bigquery_table_iam_member" "bigquery_viewer" {
  dataset_id = var.bigquery_table.dataset_id
  table_id   = var.bigquery_table.id
  role       = "roles/bigquery.dataViewer"
  member     = module.simulator_user.iam_service_account.member
}
