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

module "kingdom_internal" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "internal-server"
  iam_service_account_name        = "kingdom-internal"
  iam_service_account_description = "Kingdom internal API server."
}

module "spanner_database" {
  source = "../spanner"

  database_name = "kingdom"
  spanner_instance = var.spanner_instance
  iam_service_account_member = module.kingdom_internal.iam_service_account.member
}

moved {
  from = google_spanner_database.kingdom
  to   = module.spanner_database.google_spanner_database.db
}

moved {
  from = google_spanner_database_iam_member.kingdom_internal
  to   = module.spanner_database.google_spanner_database_iam_member.grant_db_user_role
}
