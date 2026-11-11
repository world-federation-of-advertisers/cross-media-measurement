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

variable "oidc_provider_url" {
  description = "Confidential Space issuer URL, or accounts.google.com for the Google Cloud bridge."
  type        = string
  nullable    = false
}

variable "oidc_audiences" {
  description = "Allowed OIDC audiences. An existing provider must already allow these audiences."
  type        = list(string)
  nullable    = false
}

variable "oidc_thumbprints" {
  description = "Issuer CA thumbprints required by AWS IAM."
  type        = list(string)
  nullable    = false
}

variable "role_name" {
  description = "Name of the role assumed by attested Halo workloads."
  type        = string
  nullable    = false
}

variable "sts_actions" {
  validation {
    condition     = contains(var.sts_actions, "sts:AssumeRoleWithWebIdentity") && alltrue([for action in var.sts_actions : contains(["sts:AssumeRoleWithWebIdentity", "sts:TagSession"], action)])
    error_message = "Allow AssumeRoleWithWebIdentity and optionally TagSession."
  }
  description = "AssumeRoleWithWebIdentity and, for direct attestation session tags, TagSession."
  type        = set(string)
  nullable    = false
}

variable "trust_condition" {
  validation {
    condition     = can(jsondecode(var.trust_condition)) ? length(jsondecode(var.trust_condition)) > 0 : false
    error_message = "Specify a nonempty IAM Condition JSON object."
  }
  description = "JSON IAM Condition restricting the issuer claims or attestation session tags."
  type        = string
  nullable    = false
}

variable "kms_key_description" {
  description = "Description of the EDP-owned symmetric encryption key."
  type        = string
  nullable    = false
}

variable "kms_key_alias" {
  description = "Alias beginning with alias/ for the EDP-owned key."
  type        = string
  nullable    = false
}

variable "enable_key_rotation" {
  description = "Whether AWS automatically rotates the key."
  type        = bool
  nullable    = false
}

variable "deletion_window_in_days" {
  validation {
    condition     = var.deletion_window_in_days >= 7 && var.deletion_window_in_days <= 30
    error_message = "The deletion window must be between 7 and 30 days."
  }
  description = "Waiting period, from 7 to 30 days, if key destruction is explicitly enabled."
  type        = number
  nullable    = false
}

variable "key_administrator_arns" {
  validation {
    condition     = length(var.key_administrator_arns) > 0
    error_message = "Specify at least one key administrator."
  }
  description = "Principals allowed to administer the KMS key; include the Terraform administrator."
  type        = set(string)
  nullable    = false
}

variable "workload_kms_actions" {
  validation {
    condition     = length(var.workload_kms_actions) > 0 && alltrue([for action in var.workload_kms_actions : contains(["kms:Encrypt", "kms:Decrypt"], action)])
    error_message = "Specify only the required kms:Encrypt and/or kms:Decrypt actions."
  }
  description = "Encrypt and/or decrypt actions needed by the attested workload."
  type        = set(string)
  nullable    = false
}

variable "export_principal_permissions" {
  validation {
    condition     = alltrue([for actions in values(var.export_principal_permissions) : length(actions) > 0 && alltrue([for action in actions : contains(["kms:Encrypt", "kms:Decrypt"], action)])])
    error_message = "Export grants may contain only kms:Encrypt and/or validation kms:Decrypt."
  }
  description = "Export principal ARNs mapped to their encrypt and/or validation decrypt permissions."
  type        = map(set(string))
  nullable    = false
}

variable "existing_oidc_provider_arn" {
  description = "Existing issuer provider ARN, or null to create a provider. Only one provider per issuer can exist in an AWS account."
  type        = string
  nullable    = true
}
