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
  required_version = ">= 1.7"
  backend "gcs" {}
  required_providers { google = { source = "hashicorp/google", version = "~> 7.0" } }
}
provider "google" { project = var.project_id }
variable "project_id" { type = string }
variable "signing_service_account" { type = string }
variable "jobs" { type = set(string) }
variable "key_suffix" { type = string }
data "google_kms_key_ring" "signing" {
  name     = "image-signing"
  location = "global"
}

resource "google_kms_crypto_key" "job" {
  for_each                      = var.jobs
  name                          = "tee-${each.key}-${var.key_suffix}"
  key_ring                      = data.google_kms_key_ring.signing.id
  purpose                       = "ASYMMETRIC_SIGN"
  skip_initial_version_creation = true
  version_template {
    algorithm        = "EC_SIGN_P256_SHA256"
    protection_level = "SOFTWARE"
  }
  lifecycle { prevent_destroy = true }
}
resource "google_kms_crypto_key_version" "job" {
  for_each   = var.jobs
  crypto_key = google_kms_crypto_key.job[each.key].id
  lifecycle { prevent_destroy = true }
}
resource "google_kms_crypto_key_iam_member" "signer" {
  for_each      = var.jobs
  crypto_key_id = google_kms_crypto_key.job[each.key].id
  role          = "roles/cloudkms.signerVerifier"
  member        = "serviceAccount:${google_service_account.signer.email}"
}
resource "google_kms_crypto_key_iam_member" "public_key" {
  for_each      = var.jobs
  crypto_key_id = google_kms_crypto_key.job[each.key].id
  role          = "roles/cloudkms.publicKeyViewer"
  member        = "serviceAccount:${google_service_account.signer.email}"
}
output "signing_keys" { value = { for job, version in google_kms_crypto_key_version.job : job => { name = version.name, public_key = version.public_key[0].pem } } }

resource "google_service_account" "signer" {
  account_id   = var.signing_service_account
  display_name = "TEE job image signing"
}
resource "google_iam_workload_identity_pool" "signing" {
  workload_identity_pool_id = "tee-job-signing"
  display_name              = "TEE job image signing"
}
resource "google_iam_workload_identity_pool_provider" "signing" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.signing.workload_identity_pool_id
  workload_identity_pool_provider_id = "deploy-edp-kms"
  attribute_mapping                  = { "google.subject" = "assertion.sub", "attribute.repository" = "assertion.repository" }
  attribute_condition                = "assertion.repository == 'world-federation-of-advertisers/cross-media-measurement' && ((has(assertion.job_workflow_ref) && assertion.job_workflow_ref == 'world-federation-of-advertisers/cross-media-measurement/.github/workflows/deploy-edp-kms.yml@refs/heads/main') || (!has(assertion.job_workflow_ref) && assertion.workflow_ref == 'world-federation-of-advertisers/cross-media-measurement/.github/workflows/deploy-edp-kms.yml@refs/heads/main' && assertion.ref == 'refs/heads/main'))"
  oidc { issuer_uri = "https://token.actions.githubusercontent.com" }
}
resource "google_service_account_iam_member" "signing" {
  service_account_id = google_service_account.signer.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.signing.name}/attribute.repository/world-federation-of-advertisers/cross-media-measurement"
  depends_on         = [google_iam_workload_identity_pool_provider.signing]
}
output "signing_identity" {
  value = {
    service_account            = google_service_account.signer.email
    workload_identity_provider = google_iam_workload_identity_pool_provider.signing.name
  }
}
