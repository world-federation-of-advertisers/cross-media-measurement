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

resource "google_kms_key_ring" "edp_keyring" {
  name     = var.kms_keyring_name
  location = var.region
}

resource "google_kms_crypto_key" "primus_encryption_key" {
  name            = var.kms_key_name
  key_ring        = google_kms_key_ring.primus_keyring.id
  purpose         = "ENCRYPT_DECRYPT"
  rotation_period = "7776000s" # 90 days
  lifecycle {
    prevent_destroy = true
  }
}

resource "google_service_account" "primus_sa" {
  account_id   = var.service_account_name
  display_name = "Primus Bank Service Account"
}

resource "google_kms_crypto_key_iam_member" "sa_decrypt_binding" {
  crypto_key_id = google_kms_crypto_key.primus_encryption_key.id
  role          = "roles/cloudkms.cryptoKeyDecrypter"
  member        = "serviceAccount:${google_service_account.primus_sa.email}"
}

resource "google_iam_workload_identity_pool" "primus_wip" {
  workload_identity_pool_id = var.workload_identity_pool
  display_name              = "Primus Workload Identity Pool"
  location                  = var.region
}

data "google_project" "project" {
  project_id = var.project_id
}

resource "google_service_account_iam_member" "attach_sa_to_wip" {
  service_account_id = google_service_account.primus_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/projects/${data.google_project.project.number}/locations/${var.region}/workloadIdentityPools/${var.workload_identity_pool}/*"
}