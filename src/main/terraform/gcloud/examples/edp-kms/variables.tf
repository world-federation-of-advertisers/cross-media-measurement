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

variable "project_id" {
  description = "EDP-owned Google Cloud project ID."
  type        = string
  nullable    = false
}

variable "kms_location" {
  description = "KMS key ring location."
  type        = string
  nullable    = false
}

variable "kms_keyring" {
  description = "KMS key ring name."
  type        = string
  nullable    = false
}

variable "kms_key" {
  description = "KMS crypto key name."
  type        = string
  nullable    = false
}

variable "rotation_period" {
  description = "KMS key rotation period in seconds."
  type        = string
  nullable    = false
}

variable "service_account_name" {
  description = "EDP-owned federated service account ID."
  type        = string
  nullable    = false
}

variable "service_account_display_name" {
  description = "Federated service account display name."
  type        = string
  nullable    = false
}

variable "workload_identity_pool_id" {
  description = "Workload identity pool ID."
  type        = string
  nullable    = false
}

variable "wip_display_name" {
  description = "Workload identity pool display name."
  type        = string
  nullable    = false
}

variable "workload_identity_pool_provider_id" {
  description = "Workload identity provider ID."
  type        = string
  nullable    = false
}

variable "workload_project_ids" {
  description = "Exact operator projects approved for this EDP."
  type        = set(string)
  nullable    = false
}

variable "workload_service_accounts" {
  description = "Exact runtime service account emails approved for this EDP."
  type        = set(string)
  nullable    = false
}

variable "image_signature_fingerprints" {
  description = "Approved signature_algorithm:key_id values supplied by the operator."
  type        = set(string)
  nullable    = false
}

variable "allowed_audiences" {
  description = "Accepted Confidential Space token audiences."
  type        = set(string)
  nullable    = false
}

variable "attested_kms_role" {
  description = "Selected encrypt/decrypt, decrypt-only, or encrypt-only KMS role."
  type        = string
  nullable    = false
}

variable "export_principal_roles" {
  description = "EDP export IAM members mapped to their KMS roles."
  type        = map(string)
  nullable    = false
}
