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

module "reporting_v2_cluster" {
  source = "../modules/cluster"

  name            = local.reporting_v2_cluster_name
  location        = local.cluster_location
  release_channel = var.cluster_release_channel
  secret_key      = module.common.cluster_secret_key
}

module "reporting_v2_default_node_pool" {
  source = "../modules/node-pool"

  name            = "default"
  cluster         = module.reporting_v2_cluster.cluster
  service_account = module.common.cluster_service_account
  machine_type    = "e2-custom-2-4096"
  max_node_count  = 4
}

module "reporting_v2" {
  source = "../modules/reporting"

  iam_service_account_name = "reporting-v2-internal"
  postgres_instance        = google_sql_database_instance.postgres
  postgres_database_name   = "reporting-v2"
  spanner_instance         = google_spanner_instance.spanner_instance
}

resource "google_compute_address" "reporting_v2alpha" {
  name = "reporting-v2alpha"
}
