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

  name       = local.reporting_v2_cluster_name
  location   = local.cluster_location
  secret_key = module.common.cluster_secret_key
}

data "google_container_cluster" "reporting_v2" {
  name     = local.reporting_v2_cluster_name
  location = local.cluster_location

  # Defer reading of cluster resource until it exists.
  depends_on = [module.reporting_v2_cluster]
}

module "reporting_v2_default_node_pool" {
  source = "../modules/node-pool"

  name            = "default"
  cluster         = data.google_container_cluster.reporting_v2
  service_account = module.common.cluster_service_account
  machine_type    = "e2-small"
  max_node_count  = 8
}

module "reporting_v2" {
  source = "../modules/reporting"

  iam_service_account_name = "reporting-v2-internal"
  postgres_instance        = google_sql_database_instance.postgres
  postgres_database_name   = "reporting-v2"
}
