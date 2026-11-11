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


mock_provider "aws" {
  override_during = plan
  mock_resource "aws_iam_openid_connect_provider" {
    defaults = {
      arn = "arn:aws:iam::123456789012:oidc-provider/confidentialcomputing.googleapis.com"
    }
  }
  mock_resource "aws_iam_role" {
    defaults = {
      arn = "arn:aws:iam::123456789012:role/halo-kms"
    }
  }
  mock_resource "aws_kms_key" {
    defaults = {
      arn    = "arn:aws:kms:us-east-1:123456789012:key/11111111-2222-3333-4444-555555555555"
      key_id = "11111111-2222-3333-4444-555555555555"
    }
  }
}

variables {
  existing_oidc_provider_arn = null
  oidc_provider_url          = "https://confidentialcomputing.googleapis.com"
  oidc_audiences             = ["https://example-edp.confidential-space"]
  oidc_thumbprints           = ["0123456789abcdef0123456789abcdef01234567"]
  role_name                  = "halo-kms"
  sts_actions                = ["sts:AssumeRoleWithWebIdentity", "sts:TagSession"]
  trust_condition = jsonencode({
    StringEquals = {
      "confidentialcomputing.googleapis.com:aud"             = "https://example-edp.confidential-space"
      "aws:RequestTag/swname"                                = "CONFIDENTIAL_SPACE"
      "aws:RequestTag/confidential_space.support_attributes" = "LATEST=STABLE=USABLE"
      "aws:RequestTag/container.signatures.key_ids"          = ["example-signing-key"]
      "aws:RequestTag/gce.project_id"                        = ["example-operator-project"]
    }
  })
  kms_key_description     = "EDP KEK"
  kms_key_alias           = "alias/edp-kek"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  key_administrator_arns  = ["arn:aws:iam::123456789012:root"]
  workload_kms_actions    = ["kms:Decrypt"]
  export_principal_permissions = {
    "arn:aws:iam::123456789012:role/export" = ["kms:Encrypt"]
  }
}

run "direct_confidential_space_permissions" {
  module {
    source = "../../../../main/terraform/aws/modules/edp-kms"
  }
  command = plan
  assert {
    condition     = jsondecode(aws_iam_role.attested_workload.assume_role_policy).Statement[0].Condition == jsondecode(var.trust_condition)
    error_message = "The AWS role must retain all supplied audience and attestation restrictions."
  }
  assert {
    condition     = contains(jsondecode(aws_iam_role.attested_workload.assume_role_policy).Statement[0].Action, "sts:TagSession")
    error_message = "Direct federation must accept the attestation session tags."
  }
  assert {
    condition     = jsondecode(aws_kms_key.kek.policy).Statement[1].Action == ["kms:Decrypt"] && jsondecode(aws_kms_key.kek.policy).Statement[2].Action == ["kms:Encrypt"]
    error_message = "The workload and exporter must receive distinct, minimal KMS permissions."
  }
}

run "reuse_google_bridge_provider" {
  module {
    source = "../../../../main/terraform/aws/modules/edp-kms"
  }
  command = plan
  variables {
    existing_oidc_provider_arn = "arn:aws:iam::123456789012:oidc-provider/accounts.google.com"
    oidc_provider_url          = "https://accounts.google.com"
    oidc_audiences             = ["123456789012345678901"]
    sts_actions                = ["sts:AssumeRoleWithWebIdentity"]
    workload_kms_actions       = ["kms:Decrypt", "kms:Encrypt"]
    trust_condition = jsonencode({
      StringEquals = {
        "accounts.google.com:sub" = "123456789012345678901"
        "accounts.google.com:aud" = "123456789012345678901"
      }
    })
  }
  assert {
    condition     = length(aws_iam_openid_connect_provider.attestation) == 0 && jsondecode(aws_iam_role.attested_workload.assume_role_policy).Statement[0].Principal.Federated == var.existing_oidc_provider_arn
    error_message = "A shared issuer provider must be reused without creating a duplicate."
  }
  assert {
    condition     = jsondecode(aws_iam_role.attested_workload.assume_role_policy).Statement[0].Condition.StringEquals["accounts.google.com:sub"] == "123456789012345678901"
    error_message = "The bridge trust must require the EDP service account's numeric subject."
  }
}

run "reject_workload_wildcard_permissions" {
  module {
    source = "../../../../main/terraform/aws/modules/edp-kms"
  }
  command = plan
  variables {
    workload_kms_actions = ["kms:*"]
  }
  expect_failures = [var.workload_kms_actions]
}

run "reject_empty_trust_condition" {
  module {
    source = "../../../../main/terraform/aws/modules/edp-kms"
  }
  command = plan
  variables {
    trust_condition = "{}"
  }
  expect_failures = [var.trust_condition]
}
