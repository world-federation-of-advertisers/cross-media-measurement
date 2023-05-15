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

variable "cluster_location" {
  description = "Location of Kubernetes clusters. Defaults to provider zone."
  type        = string
  default     = null
}

variable "key_ring_name" {
  description = "Name of the KMS key ring."
  type        = string
}

variable "key_ring_location" {
  description = "Location of the KMS key ring. Defaults to provider region."
  type        = string
  default     = null
}

variable "spanner_instance_name" {
  description = "Google Cloud Spanner instance name."
  type        = string
  default     = "halo-cmms"
  nullable    = false
}

variable "spanner_instance_config" {
  description = "Google Cloud Spanner instance configuration name."
  type        = string
  default     = "regional-us-central1"
  nullable    = false
}

variable "spanner_processing_units" {
  description = "Number of processing units allocated to the Spanner instance."
  type        = number
  default     = 100
  nullable    = false
}

variable "storage_bucket_name" {
  description = "Name of Google Cloud Storage bucket."
  type        = string
  nullable    = false
}

variable "storage_bucket_location" {
  description = "Location of the Google Cloud Storage bucket. Defaults to provider region."
  type        = string
  default     = null
}

variable "postgres_instance_name" {
  description = "Name of the PostgreSQL Cloud SQL instance."
  type        = string
  default     = "halo"
  nullable    = false
}

variable "postgres_instance_tier" {
  description = "Tier (machine type) of the PostgreSQL Cloud SQL instance."
  type        = string
  default     = "db-f1-micro"
  nullable    = false
}

variable "postgres_password" {
  description = "Password for postgres user."
  type        = string
  sensitive   = true
  nullable    = false
}

variable "bigquery_dataset_id" {
  description = "ID of the existing BigQuery dataset."
  type        = string
  nullable    = false
  default     = "demo"
}

variable "bigquery_table_id" {
  description = "ID of the existing BigQuery table."
  type        = string
  nullable    = false
  default     = "labelled_events"
}
