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

data "google_project" "project" {}

resource "google_kms_key_ring" "edp_key_ring" {
  project  = data.google_project.project.name
  name     = var.kms_keyring
  location = var.location
}

resource "google_kms_crypto_key" "edp_kek" {
  name            = var.kms_key
  key_ring        = google_kms_key_ring.edp_key_ring.id
  purpose         = "ENCRYPT_DECRYPT"
  rotation_period = var.rotation_period
}

resource "google_service_account" "edp_service_account" {
  account_id   = var.service_account_name
  description  = "EDP Service Account with access to KMS"
  display_name = var.service_account_display_name
}

resource "google_kms_crypto_key_iam_member" "edp_sa_decrypter" {
  crypto_key_id = google_kms_crypto_key.edp_kek.id
  role          = "roles/cloudkms.cryptoKeyDecrypter"
  member        = "serviceAccount:${google_service_account.edp_service_account.email}"
}

resource "google_iam_workload_identity_pool" "edp_workload_identity_pool" {
  project                           = data.google_project.project.name
  location                          = var.location
  workload_identity_pool_id         = var.workload_identity_pool_id
  display_name                      = var.wip_display_name
  description                       = "EDP workload identity pool."
  disabled                          = false
}

resource "google_service_account_iam_member" "edp_sa_workload_identity_user" {
  service_account_id = google_service_account.edp_service_account.name
  role = "roles/iam.workloadIdentityUser"
  member = "principalSet://iam.googleapis.com/projects/${data.google_project.project.number}/locations/global/workloadIdentityPools/${google_iam_workload_identity_pool.edp_workload_identity_pool.workload_identity_pool_id}/*"
}
