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
  type        = string
  nullable    = false
}

variable "cloud_function_name" {
  description = "The cloud function name."
  type        = string
  nullable    = false
}

variable "trigger_bucket_name" {
  description = "The name of the Google Cloud Storage bucket that triggers the Cloud Function. The Cloud Function will be invoked when a specific file is uploaded in this bucket."
  type        = string
  nullable    = false
}

variable "entry_point" {
  description = "The name of the function (as defined in source code) that will be executed."
  type        = string
  nullable    = false
}

variable "cloud_function_source_bucket" {
  description = "Cloud Storage containing the source code."
  type        = string
  nullable    = false
}

variable "cloud_function_source_object" {
  description = "Cloud Storage object containing the source code."
  type        = string
  nullable    = false
}

