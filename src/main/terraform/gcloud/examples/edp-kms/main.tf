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

module "edp_kms" {
  source = "../../modules/edp-aggregator-edp-deployment"

  location                           = var.kms_location
  kms_keyring                        = var.kms_keyring
  kms_key                            = var.kms_key
  rotation_period                    = var.rotation_period
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
  allowed_audiences = var.allowed_audiences
  attested_kms_role = var.attested_kms_role
  additional_kms_iam_members = {
    for member, role in var.export_principal_roles : member => {
      member = member
      role   = role
    }
  }
}
