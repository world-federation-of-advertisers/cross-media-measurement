# Copyright 2023 The Cross-Media Measurement Authors
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

data "google_project" "project" {}

locals {
  kms_keyring_name                = "${var.simulator_name}-key-ring"
  kms_key_name                    = "${var.simulator_name}-kek"
  edp_service_account_name        = "${var.simulator_name}"
  tee_decrypter_account_name      = "${var.simulator_name}-kms-decrypt"
  workload_identity_pool_id       = "${var.simulator_name}-wip"
  workload_identity_pool_name     = "${var.simulator_name}-wip"
  workload_identity_provider_id   = "trustee-provider"
  workload_identity_provider_name = "trustee-provider"
}

resource "google_service_account" "edp_service_account" {
  account_id   = local.edp_service_account_name
  description  = "Main service account for EDP simulator: ${var.simulator_name}"
  display_name = "EDP Simulator SA (${var.simulator_name})"
}

resource "google_service_account" "tee_decrypter_account" {
  account_id   = local.tee_decrypter_account_name
  description  = "TEE Decrypter Service Account for ${var.simulator_name}"
  display_name = "TEE Decrypter SA for ${var.simulator_name}"
}

module "workload_identity_binding" {
  source = "../workload-identity-user"

  iam_service_account      = google_service_account.edp_service_account
  k8s_service_account_name = var.simulator_name
}

resource "google_kms_key_ring" "edp_key_ring" {
  project  = data.google_project.project.name
  name     = local.kms_keyring_name
  location = var.key_ring_location
}

resource "google_kms_crypto_key" "edp_kek" {
  name            = local.kms_key_name
  key_ring        = google_kms_key_ring.edp_key_ring.id
  purpose         = "ENCRYPT_DECRYPT"
  rotation_period = var.rotation_period
}

resource "google_kms_crypto_key_iam_member" "edp_sa_kms_admin" {
  crypto_key_id = google_kms_crypto_key.edp_kek.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:${google_service_account.edp_service_account.email}"
}

resource "google_kms_crypto_key_iam_member" "tee_sa_decrypter" {
  crypto_key_id = google_kms_crypto_key.edp_kek.id
  role          = "roles/cloudkms.cryptoKeyDecrypter"
  member        = "serviceAccount:${google_service_account.tee_decrypter_account.email}"
}

resource "google_iam_workload_identity_pool" "edp_workload_identity_pool" {
  project                           = data.google_project.project.name
  workload_identity_pool_id         = local.workload_identity_pool_id
  display_name                      = local.workload_identity_pool_name
  description                       = "EDP workload identity pool for ${var.simulator_name}"
  disabled                          = false
}

resource "google_iam_workload_identity_pool_provider" "oidc_provider" {
  project                            = google_iam_workload_identity_pool.edp_workload_identity_pool.project
  workload_identity_pool_id          = google_iam_workload_identity_pool.edp_workload_identity_pool.workload_identity_pool_id
  workload_identity_pool_provider_id = local.workload_identity_provider_id
  display_name                       = local.workload_identity_provider_name

  attribute_mapping = {
    "google.subject" = "assertion.sub"
  }

  attribute_condition = "assertion.swname == 'CONFIDENTIAL_SPACE' && ['${var.tee_image_signature_fingerprint}'].exists(fingerprint, fingerprint in assertion.submods.container.image_signatures.map(sig,sig.signature_algorithm+':'+sig.key_id))"

  oidc {
    issuer_uri        = var.issuer_uri
    allowed_audiences = var.allowed_audiences
  }
}

resource "google_service_account_iam_member" "workload_identity_user_binding" {
  service_account_id = google_service_account.tee_decrypter_account.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/projects/${data.google_project.project.number}/locations/global/workloadIdentityPools/${google_iam_workload_identity_pool.edp_workload_identity_pool.workload_identity_pool_id}/*"
}