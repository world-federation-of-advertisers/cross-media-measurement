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

variable "cloud_function_service_account_name" {
  description = "IAM `google_service_account.name`."
  type = string
  nullable = false
}

variable "cloud_function_name" {
  description   = "The cloud function name."
  type          = string
  nullable      = false

variable "runtine" {
  description   = "The runtime in which the function is going to run."
  type          = string
  nullable      = false
}

variable "runtime" {
  description   = "Name of the function that will be executed when the Google Cloud Function is triggered."
  type          = string
  nullable      = false
}

variable "trigger_bucket_name" {
  description   = "The name of the Google Cloud Storage bucket that triggers the Cloud Function. The Cloud Function will be invoked when a specific file is uploaded in this bucket."
  type          = string
  nullable      = false
}

variable "docker_registry" {
  description = "Docker Registry to use."
  type        = string
  nullable    = false
  default     = "ARTIFACT_REGISTRY"

  validation {
    condition     = contains(["ARTIFACT_REGISTRY", "CONTAINER_REGISTRY"], var.docker_registry)
    error_message = "The docker_registry must be either 'ARTIFACT_REGISTRY' or 'CONTAINER_REGISTRY'."
  }
}

variable "docker_repository" {
  description   = "The full image path."
  type          = string
  nullable      = false
}
