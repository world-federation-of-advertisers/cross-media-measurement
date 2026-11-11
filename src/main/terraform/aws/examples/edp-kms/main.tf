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

provider "aws" {
  region = var.region
}

module "edp_kms" {
  source = "../../modules/edp-kms"

  existing_oidc_provider_arn = var.existing_oidc_provider_arn
  oidc_provider_url          = "https://confidentialcomputing.googleapis.com"
  oidc_audiences             = [var.audience]
  oidc_thumbprints           = var.oidc_thumbprints
  role_name                  = var.role_name
  sts_actions                = ["sts:AssumeRoleWithWebIdentity", "sts:TagSession"]
  trust_condition = jsonencode({
    StringEquals = {
      "confidentialcomputing.googleapis.com:aud"             = var.audience
      "aws:RequestTag/swname"                                = "CONFIDENTIAL_SPACE"
      "aws:RequestTag/confidential_space.support_attributes" = "LATEST=STABLE=USABLE"
      "aws:RequestTag/container.signatures.key_ids"          = var.image_signature_key_id_tags
      "aws:RequestTag/gce.project_id"                        = var.workload_project_ids
    }
  })
  kms_key_description          = var.kms_key_description
  kms_key_alias                = var.kms_key_alias
  enable_key_rotation          = var.enable_key_rotation
  deletion_window_in_days      = var.deletion_window_in_days
  key_administrator_arns       = var.key_administrator_arns
  workload_kms_actions         = var.workload_kms_actions
  export_principal_permissions = var.export_principal_permissions
}
