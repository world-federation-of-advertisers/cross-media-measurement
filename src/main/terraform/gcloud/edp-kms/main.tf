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
    prefix = "terraform/state/edp-kms/google"
  }
}

provider "google" {
  project = local.project_id
}

locals {
  input_key_id         = trimprefix(var.input_kek_uri, "gcp-kms://")
  key_parts            = split("/", local.input_key_id)
  project_id           = local.key_parts[1]
  key_location         = local.key_parts[3]
  key_ring             = local.key_parts[5]
  key_name             = local.key_parts[7]
  provider_prefix      = "projects/${var.project_number}/locations/global/workloadIdentityPools/${var.workload_identity_pool_id}/providers"
  attribute_conditions = module.attestation_policy.attribute_conditions
}

module "edp_kms" {
  source = "../modules/edp-aggregator-edp-deployment"

  location                           = local.key_location
  kms_keyring                        = local.key_ring
  kms_key                            = local.key_name
  rotation_period                    = null
  service_account_name               = var.service_account_name
  service_account_display_name       = ""
  workload_identity_pool_id          = var.workload_identity_pool_id
  wip_display_name                   = "Edp Workload Identity Pool"
  workload_identity_pool_provider_id = var.workloads.dev.provider_id
  attribute_condition                = local.attribute_conditions.dev
  allowed_audiences                  = ["https://sts.googleapis.com"]
  attested_kms_role                  = "roles/cloudkms.cryptoKeyDecrypter"
  additional_kms_iam_members = merge({
    workload_encrypt = {
      member = "serviceAccount:${var.service_account_name}@${local.project_id}.iam.gserviceaccount.com"
      role   = "roles/cloudkms.cryptoKeyEncrypter"
    }
    }, { for member in var.additional_input_key_members : member => {
      member = member
      role   = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  } })
}

resource "google_iam_workload_identity_pool_provider" "environment" {
  for_each = { for env, workload in var.workloads : env => workload if env != "dev" }

  workload_identity_pool_id          = var.workload_identity_pool_id
  workload_identity_pool_provider_id = each.value.provider_id
  display_name                       = each.value.provider_id
  attribute_mapping                  = { "google.subject" = "assertion.sub" }
  attribute_condition                = local.attribute_conditions[each.key]
  oidc {
    issuer_uri        = "https://confidentialcomputing.googleapis.com/"
    allowed_audiences = ["https://sts.googleapis.com"]
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "google_kms_crypto_key" "trustee" {
  name     = var.trustee_key_name
  key_ring = module.edp_kms.key_ring_id
  purpose  = "ENCRYPT_DECRYPT"
  lifecycle {
    prevent_destroy = true
  }
}

resource "google_kms_crypto_key_iam_member" "trustee" {
  crypto_key_id = google_kms_crypto_key.trustee.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:${module.edp_kms.service_account_email}"
}

module "attestation_policy" {
  source                         = "../modules/edp-kms-attestation-policy"
  workloads                      = var.workloads
  workload_service_account_names = var.workload_service_account_names
}
