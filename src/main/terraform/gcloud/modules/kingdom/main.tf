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

module "kingdom_internal" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "internal-server"
  iam_service_account_name        = "kingdom-internal"
  iam_service_account_description = "Kingdom internal API server."
}

resource "google_project_iam_member" "kingdom_internal_metric_writer" {
  project = data.google_project.project.name
  role    = "roles/monitoring.metricWriter"
  member  = module.kingdom_internal.iam_service_account.member
}

resource "google_spanner_database" "kingdom" {
  instance         = var.spanner_instance.name
  name             = var.spanner_database_name
  database_dialect = "GOOGLE_STANDARD_SQL"
}

resource "google_spanner_database_iam_member" "kingdom_internal" {
  instance = google_spanner_database.kingdom.instance
  database = google_spanner_database.kingdom.name
  role     = "roles/spanner.databaseUser"
  member   = module.kingdom_internal.iam_service_account.member

  lifecycle {
    replace_triggered_by = [google_spanner_database.kingdom.id]
  }
}

resource "google_compute_address" "v2alpha" {
  name    = "kingdom-v2alpha"
  address = var.v2alpha_ip_address
}

resource "google_compute_address" "system_v1alpha" {
  name    = "kingdom-system-v1alpha"
  address = var.system_v1alpha_ip_address
}

resource "google_bigquery_dataset" "operational_metrics" {
  dataset_id                      = "operational_metrics"
  friendly_name                   = "Operational Metrics"
  description                     = "Contains tables for operational metrics"
  default_partition_expiration_ms = 31622400000 // 366 days
}

resource "google_bigquery_table" "measurements" {
  dataset_id = google_bigquery_dataset.operational_metrics.dataset_id
  table_id   = "measurements"

  deletion_protection = true

  time_partitioning {
    field = "update_time"
    type  = "MONTH"
  }

  schema = <<EOF
[
  {
    "name": "measurement_consumer_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "measurement_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "is_direct",
    "type": "BOOLEAN",
    "mode": "REQUIRED",
    "defaultValueExpression": "FALSE"
  },
  {
    "name": "measurement_type",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "REACH_AND_FREQUENCY, REACH, IMPRESSION, DURATION, or POPULATION"
  },
  {
    "name": "state",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "SUCCEEDED or FAILED"
  },
  {
    "name": "create_time",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "update_time",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "completion_duration_seconds",
    "type": "INTEGER",
    "mode": "REQUIRED",
    "defaultValueExpression": "0"
  },
  {
    "name": "completion_duration_seconds_squared",
    "type": "INTEGER",
    "mode": "REQUIRED",
    "defaultValueExpression": "0"
  }
]
EOF

}

resource "google_bigquery_table" "requisitions" {
  dataset_id = google_bigquery_dataset.operational_metrics.dataset_id
  table_id   = "requisitions"

  deletion_protection = true

  time_partitioning {
    field = "update_time"
    type  = "MONTH"
  }

  schema = <<EOF
[
  {
    "name": "measurement_consumer_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "measurement_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "requisition_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "data_provider_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "is_direct",
    "type": "BOOLEAN",
    "mode": "REQUIRED",
    "defaultValueExpression": "FALSE"
  },
  {
    "name": "measurement_type",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "REACH_AND_FREQUENCY, REACH, IMPRESSION, DURATION, or POPULATION"
  },
  {
    "name": "state",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "FULFILLED or REFUSED"
  },
  {
    "name": "create_time",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "update_time",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "completion_duration_seconds",
    "type": "INTEGER",
    "mode": "REQUIRED",
    "defaultValueExpression": "0"
  },
  {
    "name": "completion_duration_seconds_squared",
    "type": "INTEGER",
    "mode": "REQUIRED",
    "defaultValueExpression": "0"
  },
  {
    "name": "build_label",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "warnings",
    "type": "STRING",
    "mode": "REPEATED"
  }
]
EOF

}

resource "google_bigquery_table" "computation_participant_stages" {
  dataset_id = google_bigquery_dataset.operational_metrics.dataset_id
  table_id   = "computation_participant_stages"

  deletion_protection = true

  time_partitioning {
    field = "stage_start_time"
    type  = "MONTH"
  }

  schema = <<EOF
[
  {
    "name": "measurement_consumer_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "measurement_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "computation_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "duchy_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "measurement_type",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "REACH_AND_FREQUENCY and REACH"
  },
  {
    "name": "result",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "SUCCEEDED or FAILED"
  },
  {
    "name": "stage_name",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "stage_start_time",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "completion_duration_seconds",
    "type": "INTEGER",
    "mode": "REQUIRED",
    "description": "Difference between start of this stage and start of next stage",
    "defaultValueExpression": "0"
  },
  {
    "name": "completion_duration_seconds_squared",
    "type": "INTEGER",
    "mode": "REQUIRED",
    "defaultValueExpression": "0"
  }
]
EOF

}

resource "google_bigquery_table" "latest_measurement_read" {
  dataset_id = google_bigquery_dataset.operational_metrics.dataset_id
  table_id   = "latest_measurement_read"

  deletion_protection = true

  time_partitioning {
    expiration_ms = 3888000000 // 45 days
    type          = "MONTH"
  }

  schema = <<EOF
[
  {
    "name": "update_time",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "external_measurement_consumer_id",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "external_measurement_id",
    "type": "INTEGER",
    "mode": "REQUIRED"
  }
]
EOF

}

resource "google_bigquery_table" "latest_requisition_read" {
  dataset_id = google_bigquery_dataset.operational_metrics.dataset_id
  table_id   = "latest_requisition_read"

  deletion_protection = true

  time_partitioning {
    expiration_ms = 3888000000 // 45 days
    type          = "MONTH"
  }

  schema = <<EOF
[
  {
    "name": "update_time",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "external_data_provider_id",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "external_requisition_id",
    "type": "INTEGER",
    "mode": "REQUIRED"
  }
]
EOF

}

resource "google_bigquery_table" "latest_computation_read" {
  dataset_id = google_bigquery_dataset.operational_metrics.dataset_id
  table_id   = "latest_computation_read"

  deletion_protection = true

  time_partitioning {
    expiration_ms = 3888000000 // 45 days
    type          = "MONTH"
  }

  schema = <<EOF
[
  {
    "name": "update_time",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "external_computation_id",
    "type": "INTEGER",
    "mode": "REQUIRED",
    "defaultValueExpression": "0"
  }
]
EOF

}

module "kingdom_operational_metrics" {
  source = "../workload-identity-user"

  k8s_service_account_name        = "operational-metrics"
  iam_service_account_name        = "operational-metrics"
  iam_service_account_description = "Operational Metrics Cron Job"
}

resource "google_bigquery_dataset_iam_member" "bigquery_writer" {
  dataset_id = google_bigquery_dataset.operational_metrics.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = module.kingdom_operational_metrics.iam_service_account.member
}

resource "google_project_iam_member" "bigquery_job_user" {
  project = data.google_project.project.project_id
  role    = "roles/bigquery.jobUser"
  member  = module.kingdom_operational_metrics.iam_service_account.member
}


resource "google_monitoring_dashboard" "dashboards" {
  for_each = toset(var.dashboard_json_files)

  dashboard_json = file("${path.module}/${each.value}")
  project        = data.google_project.project.project_id
}
