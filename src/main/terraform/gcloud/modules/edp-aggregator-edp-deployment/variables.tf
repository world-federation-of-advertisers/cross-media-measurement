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

variable "location" {
  description = "Location used for both KMS resources and workload identity pool."
  type        = string
  default     = "global"
}

variable "kms_keyring" {
  description = "KMS key ring name."
  type        = string
  nullable    = false
}

variable "kms_key" {
  description = "EDP 'KEK' key name."
  type        = string
  nullable    = false
}

variable "rotation_period" {
  description = "Rotation period for the KMS crypto key"
  type        = string
  default     = "63072000s" # 2 years
}

variable "service_account_name" {
  description = "IAM `google_service_account.name`."
  type        = string
  nullable    = false
}

variable "service_account_display_name" {
  description = "EDP service account display name."
  type        = string
}

variable "workload_identity_pool_id" {
  description = "Workload Identity Pool ID."
  type        = string
  validation {
    condition     = length(var.workload_identity_pool_id) >= 4 && length(var.workload_identity_pool_id) <= 32
    error_message = "Workload identity pool ID must be between 4 and 32 characters."
  }
  nullable    = false
}

variable "wip_display_name" {
  description = "EDP WIP display name."
  type        = string
}