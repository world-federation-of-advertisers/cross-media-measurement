# Copyright 2025 The Cross-Media Measurement Authors
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

variable "scheduler_service_account_name" {
  description = "Name of the service account for the Cloud Scheduler job"
  type        = string
  nullable    = false
}

variable "job_name" {
  description = "Name of the Cloud Scheduler job"
  type        = string
  nullable    = false
}

variable "job_description" {
  description = "Description of the Cloud Scheduler job"
  type        = string
  default     = "Cloud Scheduler job"
  nullable    = false
}

variable "schedule" {
  description = "Schedule in cron format"
  type        = string
  nullable    = false
}

variable "time_zone" {
  description = "Time zone for the scheduler"
  type        = string
  default     = "UTC"
  nullable    = false
}

variable "attempt_deadline" {
  description = "The deadline for job attempts in seconds"
  type        = string
  default     = "180s"
  nullable    = false
}

variable "function_uri" {
  description = "URI of the cloud function to be triggered"
  type        = string
  nullable    = false
}

variable "cloud_function_name" {
  description = "Name of the cloud function to be triggered"
  type        = string
  nullable    = false
}

variable "terraform_service_account" {
  description = "Service account used by terraform that needs to attach the scheduler service account"
  type        = string
  nullable    = false
}

variable "region" {
  description = "GCP region where the cloud function is deployed"
  type        = string
  nullable    = false
}