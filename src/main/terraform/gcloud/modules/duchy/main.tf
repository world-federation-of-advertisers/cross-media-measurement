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

module "gmp_monitoring" {
  source = "../workload-identity-user"

  k8s_service_account_name = "gmp-monitoring"
  iam_service_account      = var.monitoring_service_account
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

resource "google_spanner_database" "db" {
  instance         = var.spanner_instance.name
  name             = local.database_name
  database_dialect = "GOOGLE_STANDARD_SQL"
}

resource "google_spanner_database_iam_member" "internal_server" {
  instance = google_spanner_database.db.instance
  database = google_spanner_database.db.name
  role     = "roles/spanner.databaseUser"
  member   = module.internal_server_user.iam_service_account.member
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
