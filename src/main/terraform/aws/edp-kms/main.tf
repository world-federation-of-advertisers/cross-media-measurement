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

terraform {
  backend "gcs" {
    prefix = "terraform/state/edp-kms/aws"
  }
}
provider "aws" {
  region              = var.region
  allowed_account_ids = [local.account_id]
}
locals {
  key_arn    = trimprefix(var.input_kek_uri, "aws-kms://")
  account_id = split(":", local.key_arn)[4]
  key_id     = split("/", local.key_arn)[1]
  key_ids    = sort(tolist(var.image_signature_key_ids))
  key_id_tags = concat(local.key_ids, [
    for pair in setproduct(local.key_ids, local.key_ids) : join("=", pair) if pair[0] != pair[1]
  ])
}

module "edp_kms" {
  source = "../modules/edp-kms"

  existing_oidc_provider_arn = aws_iam_openid_connect_provider.attestation.arn
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
      "aws:RequestTag/container.signatures.key_ids"          = local.key_id_tags
      "aws:RequestTag/gce.project_id"                        = var.workload_project_ids
    }
  })
  kms_key_description          = "KMS key for GenerateSyntheticData envelope encryption"
  kms_key_alias                = var.key_alias
  enable_key_rotation          = false
  deletion_window_in_days      = 30
  key_administrator_arns       = ["arn:aws:iam::${local.account_id}:root"]
  workload_kms_actions         = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey"]
  export_principal_permissions = {}
  key_policy_json = jsonencode({
    Version = "2012-10-17"
    Id      = "key-default-1"
    Statement = [{
      Sid       = "Enable IAM User Permissions"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${local.account_id}:root" }
      Action    = "kms:*"
      Resource  = "*"
    }]
  })
}

resource "aws_iam_openid_connect_provider" "attestation" {
  url             = "https://confidentialcomputing.googleapis.com"
  client_id_list  = [var.audience]
  thumbprint_list = var.oidc_thumbprints
}

resource "aws_iam_role_policy" "attested_workload" {
  name = "Policy"
  role = module.edp_kms.role_name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "Statement1"
      Effect   = "Allow"
      Action   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"]
      Resource = local.key_arn
    }]
  })
}

resource "aws_iam_role" "export" {
  name        = var.export_role_name
  description = "Role for GCE VM to use KMS for GenerateSyntheticData"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = "arn:aws:iam::${local.account_id}:oidc-provider/accounts.google.com" }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = { StringEquals = { "accounts.google.com:sub" = var.export_google_subjects } }
    }]
  })
}
resource "aws_iam_role_policy" "export" {
  name = "KmsAccess"
  role = aws_iam_role.export.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey", "kms:GenerateDataKeyWithoutPlaintext", "kms:DescribeKey"]
      Resource = local.key_arn
    }]
  })
}
