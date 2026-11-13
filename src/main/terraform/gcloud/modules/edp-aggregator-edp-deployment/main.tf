# Copyright 2025 The Cross-Media Measurement Authors
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

resource "google_kms_key_ring" "edp_key_ring" {
  name     = var.kms_keyring
  location = var.location
}

resource "google_kms_crypto_key" "edp_kek" {
  name            = var.kms_key
  key_ring        = google_kms_key_ring.edp_key_ring.id
  purpose         = "ENCRYPT_DECRYPT"
  rotation_period = var.rotation_period

  lifecycle {
    prevent_destroy = true
  }
}

module "workload_identity" {
  source = "../confidential-space-workload-identity"

  service_account_name               = var.service_account_name
  service_account_display_name       = var.service_account_display_name
  workload_identity_pool_id          = var.workload_identity_pool_id
  wip_display_name                   = var.wip_display_name
  workload_identity_pool_provider_id = var.workload_identity_pool_provider_id
  attribute_condition                = var.attribute_condition
  allowed_audiences                  = var.allowed_audiences
  federated_service_account_role     = "roles/iam.workloadIdentityUser"
}

resource "google_kms_crypto_key_iam_member" "edp_sa_decrypter" {
  crypto_key_id = google_kms_crypto_key.edp_kek.id
  role          = var.attested_kms_role
  member        = "serviceAccount:${module.workload_identity.service_account_email}"
}

resource "google_kms_crypto_key_iam_member" "additional_access" {
  for_each      = var.additional_kms_iam_members
  crypto_key_id = google_kms_crypto_key.edp_kek.id
  role          = each.value.role
  member        = each.value.member
}
