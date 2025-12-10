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

variable "iam_service_account_name_ui" {
  description = "IAM `google_service_account.name` for the UI server."
  type        = string
  nullable    = false
}

variable "iam_service_account_name_gateway" {
  description = "IAM `google_service_account.name` for the Gateway server."
  type        = string
  nullable    = false
}

variable "iam_service_account_name_grpc" {
  description = "IAM `google_service_account.name` for the GRPC server."
  type        = string
  nullable    = false
}

variable "storage_bucket_name" {
  description = "Name of Google Cloud Storage bucket where the website and server files will be stored."
  type        = string
  nullable    = false
}

variable "storage_bucket_location" {
  description = "Location of the Google Cloud Storage bucket. Defaults to provider region."
  type        = string
  default     = null
}
