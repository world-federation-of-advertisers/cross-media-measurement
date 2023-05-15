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

terraform {
  backend "gcs" {
    prefix = "terraform/state/halo-cmms"
  }
}

locals {
  kingdom_cluster_name    = "kingdom"
  duchy_names             = toset(["aggregator", "worker1", "worker2"])
  reporting_cluster_name  = "reporting"
  simulators_cluster_name = "simulators"

  cluster_location        = var.cluster_location == null ? data.google_client_config.default.zone : var.cluster_location
  key_ring_location       = var.key_ring_location == null ? data.google_client_config.default.region : var.key_ring_location
  storage_bucket_location = var.storage_bucket_location == null ? data.google_client_config.default.region : var.storage_bucket_location
}

provider "google" {}

data "google_client_config" "default" {}

module "common" {
  source = "../modules/common"

  key_ring_name     = var.key_ring_name
  key_ring_location = local.key_ring_location
}

resource "google_spanner_instance" "spanner_instance" {
  name             = var.spanner_instance_name
  config           = var.spanner_instance_config
  display_name     = "Halo CMMS"
  processing_units = var.spanner_processing_units
}

resource "google_sql_database_instance" "postgres" {
  name             = var.postgres_instance_name
  database_version = "POSTGRES_14"
  settings {
    tier = var.postgres_instance_tier

    insights_config {
      query_insights_enabled  = true
      record_application_tags = true
    }

    database_flags {
      name  = "cloudsql.iam_authentication"
      value = "on"
    }
    database_flags {
      name  = "max_pred_locks_per_page"
      value = "64"
    }
  }
}

resource "google_sql_user" "postgres" {
  name     = "postgres"
  instance = google_sql_database_instance.postgres.name
  password = var.postgres_password
}

provider "postgresql" {
  scheme   = "gcppostgres"
  host     = google_sql_database_instance.postgres.connection_name
  username = google_sql_user.postgres.name
  password = google_sql_user.postgres.password
}
