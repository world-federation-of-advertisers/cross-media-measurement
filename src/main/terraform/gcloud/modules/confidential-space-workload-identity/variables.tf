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

variable "service_account_name" {
  description = "Account ID of the EDP-owned attested-workload service account."
  type        = string
  nullable    = false
}

variable "service_account_display_name" {
  description = "Display name of the attested-workload service account."
  type        = string
  nullable    = false
}

variable "workload_identity_pool_id" {
  description = "ID of the globally scoped workload identity pool."
  type        = string
  nullable    = false
}

variable "wip_display_name" {
  description = "Display name of the workload identity pool."
  type        = string
  nullable    = false
}

variable "workload_identity_pool_provider_id" {
  description = "ID of the Confidential Space OIDC provider."
  type        = string
  nullable    = false
}

variable "attribute_condition" {
  validation {
    condition     = length(trimspace(var.attribute_condition)) > 0
    error_message = "Specify a nonempty attestation condition."
  }
  description = "CEL condition restricting stable Confidential Space, signing keys, projects, and workload identities."
  type        = string
  nullable    = false
}

variable "allowed_audiences" {
  validation {
    condition     = length(var.allowed_audiences) > 0
    error_message = "Specify at least one allowed audience."
  }
  description = "Accepted Confidential Space token audiences."
  type        = set(string)
  nullable    = false
}

variable "federated_service_account_role" {
  validation {
    condition     = contains(["roles/iam.workloadIdentityUser", "roles/iam.serviceAccountTokenCreator"], var.federated_service_account_role)
    error_message = "Use workloadIdentityUser or serviceAccountTokenCreator."
  }
  description = "Use workloadIdentityUser for Google KMS or serviceAccountTokenCreator for the AWS bridge."
  type        = string
  nullable    = false
}
