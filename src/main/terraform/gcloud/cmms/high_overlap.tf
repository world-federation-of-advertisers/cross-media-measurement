# Copyright 2026 The Cross-Media Measurement Authors
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

# Ground truth for the high overlap synthetic dataset, written by
# SeedHighOverlapDataset. Columns mirror HighOverlapImpressionRow.

resource "google_bigquery_dataset" "high_overlap" {
  dataset_id    = "high_overlap"
  friendly_name = "High Overlap Synthetic Dataset"
  description   = "Impression-level ground truth for the high overlap test dataset"
  project       = data.google_client_config.default.project
  location      = data.google_client_config.default.region
}

resource "google_bigquery_table" "impressions" {
  dataset_id = google_bigquery_dataset.high_overlap.dataset_id
  table_id   = "impressions"
  project    = data.google_client_config.default.project

  # Regenerable by re-running the seeding job.
  deletion_protection = false

  time_partitioning {
    field = "event_time"
    type  = "DAY"
  }

  # model_line first: re-seeding deletes by it.
  clustering = ["model_line", "edp_name", "cmms_event_group_reference_id"]

  schema = <<EOF
[
  {
    "name": "model_line",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "ModelLine resource name the impressions are stamped with"
  },
  {
    "name": "spec_fingerprint",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Fingerprint of the specs these impressions were generated from"
  },
  {
    "name": "edp_name",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "logical_group",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Grouping label from the config, shared by every entity key under it"
  },
  {
    "name": "entity_type",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "entity_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "cmms_event_group_reference_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "EventGroup reference ID as registered with the Kingdom"
  },
  {
    "name": "vid",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "event_time",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "gender",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "age_group",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "us_state",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "signed_in",
    "type": "BOOLEAN",
    "mode": "NULLABLE"
  },
  {
    "name": "video_completed_fraction",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "video_viewable_fraction",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "display_viewable_fraction",
    "type": "FLOAT",
    "mode": "NULLABLE"
  }
]
EOF
}

resource "google_bigquery_dataset_iam_member" "seeder_data_editor" {
  dataset_id = google_bigquery_dataset.high_overlap.dataset_id
  project    = data.google_client_config.default.project
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.terraform_service_account}"
}

# Required to run the DELETE and COUNT queries the seeding job issues.
resource "google_project_iam_member" "seeder_job_user" {
  project = data.google_client_config.default.project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.terraform_service_account}"
}
