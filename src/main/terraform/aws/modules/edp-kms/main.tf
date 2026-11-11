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
  oidc_provider_arn = var.existing_oidc_provider_arn == null ? aws_iam_openid_connect_provider.attestation[0].arn : var.existing_oidc_provider_arn
}

resource "aws_iam_openid_connect_provider" "attestation" {
  count           = var.existing_oidc_provider_arn == null ? 1 : 0
  url             = var.oidc_provider_url
  client_id_list  = var.oidc_audiences
  thumbprint_list = var.oidc_thumbprints
}

resource "aws_iam_role" "attested_workload" {
  name = var.role_name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = local.oidc_provider_arn }
      Action    = var.sts_actions
      Condition = jsondecode(var.trust_condition)
    }]
  })
}

resource "aws_kms_key" "kek" {
  description             = var.kms_key_description
  enable_key_rotation     = var.enable_key_rotation
  deletion_window_in_days = var.deletion_window_in_days
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Sid       = "KeyAdministration"
        Effect    = "Allow"
        Principal = { AWS = var.key_administrator_arns }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid       = "AttestedWorkloadAccess"
        Effect    = "Allow"
        Principal = { AWS = aws_iam_role.attested_workload.arn }
        Action    = var.workload_kms_actions
        Resource  = "*"
      }
      ], [for principal, actions in var.export_principal_permissions : {
        Effect    = "Allow"
        Principal = { AWS = principal }
        Action    = actions
        Resource  = "*"
    }])
  })

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_kms_alias" "kek" {
  name          = var.kms_key_alias
  target_key_id = aws_kms_key.kek.key_id
}
