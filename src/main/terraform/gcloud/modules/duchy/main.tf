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

locals {
  database_name = var.database_name == null ? "${var.name}-duchy" : var.database_name
}

module "storage_user" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "storage"
  iam_service_account_name        = "${var.name}-duchy-storage"
  iam_service_account_description = "${var.name} Duchy storage."
}

module "internal_server_user" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "internal-server"
  iam_service_account_name        = "${var.name}-duchy-internal"
  iam_service_account_description = "${var.name} internal API server."
}

module "spanner_database" {
  source = "../spanner"

  count = var.spanner_instance == null ? 0 : 1

  database_name = local.database_name
  spanner_instance = var.spanner_instance
  iam_service_account_member = module.internal_server_user.iam_service_account.member
}

module "postgres_database" {
  source = "../postgres"

  count = var.postgres_instance == null ? 0 : 1

  database_name = local.database_name
  postgres_instance = var.postgres_instance
  iam_service_account_email = module.internal_server_user.iam_service_account.email
  iam_service_account_member = module.internal_server_user.iam_service_account.member
}

resource "google_storage_bucket_iam_member" "internal_server" {
  bucket = var.storage_bucket.name
  role   = "roles/storage.objectAdmin"
  member = module.internal_server_user.iam_service_account.member
}

resource "google_storage_bucket_iam_member" "storage" {
  bucket = var.storage_bucket.name
  role   = "roles/storage.objectAdmin"
  member = module.storage_user.iam_service_account.member
}

moved {
  from = google_spanner_database.db
  to   = module.spanner_database[0].google_spanner_database.db
}

moved {
  from = google_spanner_database_iam_member.internal_server
  to   = module.spanner_database[0].google_spanner_database_iam_member.grant_db_user_role
}
