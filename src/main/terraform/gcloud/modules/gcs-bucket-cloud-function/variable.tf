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
  description = "The name of the service account assigned to the Cloud Function (`google_service_account.name`)."
  type        = string
  nullable    = false
}

variable "cloud_function_trigger_service_account_name" {
  description = "The name of the service account used to trigger the Cloud Function (`google_service_account.name`)."
  type        = string
  nullable    = false
}

variable "trigger_bucket_name" {
  description = "The name of the Google Cloud Storage bucket that triggers the Cloud Function. The Cloud Function will be invoked when a specific file is uploaded in this bucket."
  type        = string
  nullable    = false
}

variable "terraform_service_account" {
  description = "Service account used by terraform that needs to attach the MIG service account to the VM."
  type        = string
  nullable    = false
}

variable "function_name" {
  description = "The function name to be deployed."
  type        = string
  nullable    = false
}

variable "entry_point" {
  description = "The entry point of the main class prefixed with its package."
  type        = string
  nullable    = false
}

variable "extra_env_vars" {
  description = "Additional environment variables to be provided to the Cloud Function."
  type        = string
  nullable    = false
}

variable "secret_mappings" {
  description = "Mapping of local file system paths to Google Secret Manager secrets and versions."
  type        = string
  nullable    = false
}

variable "uber_jar_path" {
  description = "The path to the uber jar."
  type        = string
  nullable    = false
}