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

variable "region" {
  description = "EDP AWS region."
  type        = string
  nullable    = false
}

variable "audience" {
  description = "EDP-chosen Confidential Space audience; do not use https://sts.google.com."
  type        = string
  nullable    = false
}

variable "oidc_thumbprints" {
  description = "Verified Confidential Space issuer CA thumbprints."
  type        = list(string)
  nullable    = false
}

variable "role_name" {
  description = "Attested workload IAM role name."
  type        = string
  nullable    = false
}

variable "image_signature_key_id_tags" {
  description = "Exact approved signing-key tags, including sorted =-joined combinations."
  type        = set(string)
  nullable    = false
}

variable "workload_project_ids" {
  description = "Exact operator projects approved for this EDP."
  type        = set(string)
  nullable    = false
}

variable "kms_key_description" {
  description = "Description of the EDP KMS key."
  type        = string
  nullable    = false
}

variable "kms_key_alias" {
  description = "Alias of the EDP KMS key."
  type        = string
  nullable    = false
}

variable "enable_key_rotation" {
  description = "Enable automatic KMS rotation."
  type        = bool
  nullable    = false
}

variable "deletion_window_in_days" {
  description = "KMS deletion waiting period."
  type        = number
  nullable    = false
}

variable "key_administrator_arns" {
  description = "Key administrators, including the Terraform administrator."
  type        = set(string)
  nullable    = false
}

variable "workload_kms_actions" {
  description = "Encrypt and/or decrypt operations required by the selected path."
  type        = set(string)
  nullable    = false
}

variable "export_principal_permissions" {
  description = "Export principals mapped to their key operations."
  type        = map(set(string))
  nullable    = false
}

variable "existing_oidc_provider_arn" {
  description = "Existing issuer provider ARN, or null to create one."
  type        = string
  nullable    = true
}
