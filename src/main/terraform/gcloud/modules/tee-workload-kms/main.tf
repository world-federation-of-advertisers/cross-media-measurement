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

locals {
  attribute_conditions = { for job, w in var.workloads : job => join(" && ", [
    "assertion.swname == 'CONFIDENTIAL_SPACE'",
    "'STABLE' in assertion.submods.confidential_space.support_attributes",
    "assertion.submods.gce.project_id in ${jsonencode(w.workload_project_ids)}",
    "${jsonencode(w.workload_service_accounts)}.exists(sa, sa in assertion.google_service_accounts)",
    "${jsonencode(w.image_signature_fingerprints)}.exists(fp, fp in assertion.submods.container.image_signatures.map(sig, sig.signature_algorithm + ':' + sig.key_id))",
    "${jsonencode(w.image_repositories)}.exists(repo, assertion.submods.container.image_reference.startsWith(repo + ':') || assertion.submods.container.image_reference.startsWith(repo + '@'))",
  ]) }
  google_key_ids = merge({ for purpose, key in var.google_keys : purpose => key.existing_key_id if key.existing_key_id != null }, { for purpose, key in google_kms_crypto_key.purpose : purpose => key.id })
  aws_key_arns   = merge({ for purpose, key in var.aws_keys : purpose => key.existing_key_arn if key.existing_key_arn != null }, { for purpose, key in aws_kms_key.purpose : purpose => key.arn })
  google_grants = merge({}, [for job, w in var.workloads : merge({}, [for purpose, roles in w.google_key_permissions : {
    for role in roles : "${job}/${purpose}/${role}" => { job = job, purpose = purpose, role = role }
  }]...)]...)
}

module "identity" {
  for_each                           = var.workloads
  source                             = "../confidential-space-workload-identity"
  service_account_name               = each.value.service_account_name
  service_account_display_name       = "Attested ${each.key}"
  workload_identity_pool_id          = each.value.workload_identity_pool_id
  wip_display_name                   = each.key
  workload_identity_pool_provider_id = "confidential-space"
  attribute_condition                = local.attribute_conditions[each.key]
  allowed_audiences                  = ["https://sts.googleapis.com"]
  federated_service_account_role     = "roles/iam.serviceAccountTokenCreator"
}

resource "google_kms_crypto_key" "purpose" {
  for_each        = { for purpose, key in var.google_keys : purpose => key if key.existing_key_id == null }
  name            = each.value.key_name
  key_ring        = each.value.key_ring_id
  purpose         = "ENCRYPT_DECRYPT"
  rotation_period = each.value.rotation_period
  lifecycle { prevent_destroy = true }
}

resource "google_kms_crypto_key_iam_member" "workload" {
  for_each      = local.google_grants
  crypto_key_id = local.google_key_ids[each.value.purpose]
  role          = each.value.role
  member        = "serviceAccount:${module.identity[each.value.job].service_account_email}"
}

resource "aws_iam_role" "workload" {
  for_each = var.workloads
  name     = each.value.aws_role_name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = "accounts.google.com" }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = { StringEquals = {
        "accounts.google.com:sub"  = module.identity[each.key].service_account_unique_id
        "accounts.google.com:aud"  = module.identity[each.key].service_account_unique_id
        "accounts.google.com:oaud" = module.identity[each.key].service_account_unique_id
      } }
    }]
  })
}

resource "aws_kms_key" "purpose" {
  for_each                = { for purpose, key in var.aws_keys : purpose => key if key.existing_key_arn == null }
  description             = each.value.description
  enable_key_rotation     = each.value.enable_key_rotation
  deletion_window_in_days = each.value.deletion_window_in_days
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([{
      Sid       = "KeyAdministration"
      Effect    = "Allow"
      Principal = { AWS = var.key_administrator_arns }
      Action    = "kms:*"
      Resource  = "*"
      }], [for job, w in var.workloads : {
      Effect    = "Allow"
      Principal = { AWS = aws_iam_role.workload[job].arn }
      Action    = w.aws_key_permissions[each.key]
      Resource  = "*"
    } if contains(keys(w.aws_key_permissions), each.key)])
  })
  lifecycle { prevent_destroy = true }
}

resource "aws_kms_alias" "purpose" {
  for_each      = { for purpose, key in var.aws_keys : purpose => key if key.existing_key_arn == null }
  name          = each.value.alias
  target_key_id = aws_kms_key.purpose[each.key].key_id
}

resource "aws_iam_role_policy" "workload" {
  for_each = var.workloads
  name     = "PurposeSpecificKmsAccess"
  role     = aws_iam_role.workload[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [for purpose, actions in each.value.aws_key_permissions : {
      Effect   = "Allow"
      Action   = actions
      Resource = local.aws_key_arns[purpose]
    }]
  })
}
