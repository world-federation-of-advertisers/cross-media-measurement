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

provider "google" {
  project = var.project_id
}

provider "aws" {
  region = var.region
}

module "workload_identity" {
  source = "../../../gcloud/modules/confidential-space-workload-identity"

  service_account_name               = var.service_account_name
  service_account_display_name       = var.service_account_display_name
  workload_identity_pool_id          = var.workload_identity_pool_id
  wip_display_name                   = var.wip_display_name
  workload_identity_pool_provider_id = var.workload_identity_pool_provider_id
  attribute_condition = join(" && ", [
    "assertion.swname == 'CONFIDENTIAL_SPACE'",
    "'STABLE' in assertion.submods.confidential_space.support_attributes",
    "assertion.submods.gce.project_id in ${jsonencode(var.workload_project_ids)}",
    "${jsonencode(var.workload_service_accounts)}.exists(sa, sa in assertion.google_service_accounts)",
    "${jsonencode(var.image_signature_fingerprints)}.exists(fingerprint, fingerprint in assertion.submods.container.image_signatures.map(sig, sig.signature_algorithm + ':' + sig.key_id))",
  ])
  allowed_audiences              = var.allowed_audiences
  federated_service_account_role = "roles/iam.serviceAccountTokenCreator"
}

module "edp_kms" {
  source = "../../modules/edp-kms"

  existing_oidc_provider_arn = var.existing_oidc_provider_arn
  oidc_provider_url          = "https://accounts.google.com"
  oidc_audiences             = [module.workload_identity.service_account_unique_id]
  oidc_thumbprints           = var.oidc_thumbprints
  role_name                  = var.role_name
  sts_actions                = ["sts:AssumeRoleWithWebIdentity"]
  trust_condition = jsonencode({
    StringEquals = {
      "accounts.google.com:sub" = module.workload_identity.service_account_unique_id
      "accounts.google.com:aud" = module.workload_identity.service_account_unique_id
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
