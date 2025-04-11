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

variable "postgres_instance" {
  description = "PostgreSQL `google_sql_database_instance`."
  type = object({
    name            = string
    connection_name = string
  })
  nullable = false
}

variable "postgres_database_name" {
  description = "PostgreSQL `google_sql_database.name`."
  type        = string
  default     = "reporting"
  nullable    = false
}

variable "iam_service_account_name" {
  description = "IAM `google_service_account.name`."
  type        = string
  default     = "reporting-internal"
  nullable    = false
}

variable "spanner_instance" {
  description = "`google_spanner_instance` for the system."
  type = object({
    name = string
  })
  nullable = false
}

variable "reporting_spanner_database_name" {
  description = "Name of the Spanner database for Reporting."
  type        = string
  default     = "reporting"
  nullable    = false
}

variable "access_spanner_database_name" {
  description = "Name of the Spanner database for Access."
  type        = string
  default     = "access"
  nullable    = false
}

variable "dashboard_json_files" {
  description = "List of filenames of dashboard json or templates"
  type        = list(string)
  nullable    = true
  default     = [
    "reporting_dashboard_1.json",
    "reporting_dashboard_2.json",
  ]
}
