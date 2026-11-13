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


mock_provider "google" {
  override_during = plan
  mock_resource "google_service_account" {
    defaults = {
      email     = "halo-kms@example-edp-project.iam.gserviceaccount.com"
      name      = "projects/example-edp-project/serviceAccounts/halo-kms@example-edp-project.iam.gserviceaccount.com"
      unique_id = "123456789012345678901"
    }
  }
  mock_resource "google_iam_workload_identity_pool" {
    defaults = {
      name = "projects/123456789012/locations/global/workloadIdentityPools/halo-workloads"
    }
  }
  mock_resource "google_iam_workload_identity_pool_provider" {
    defaults = {
      name = "projects/123456789012/locations/global/workloadIdentityPools/halo-workloads/providers/confidential-space"
    }
  }
  mock_resource "google_kms_crypto_key" {
    defaults = {
      id = "projects/example-edp-project/locations/us-central1/keyRings/edp-key-ring/cryptoKeys/edp-kek"
    }
  }
}

variables {

  service_account_name               = "halo-kms"
  service_account_display_name       = "Halo KMS"
  workload_identity_pool_id          = "halo-workloads"
  wip_display_name                   = "Halo workloads"
  workload_identity_pool_provider_id = "confidential-space"
  attribute_condition                = "assertion.swname == 'CONFIDENTIAL_SPACE' && 'STABLE' in assertion.submods.confidential_space.support_attributes"
  allowed_audiences                  = ["https://sts.googleapis.com"]

  location          = "us-central1"
  kms_keyring       = "edp-key-ring"
  kms_key           = "edp-kek"
  attested_kms_role = "roles/cloudkms.cryptoKeyDecrypter"
  additional_kms_iam_members = {
    export = {
      role   = "roles/cloudkms.cryptoKeyEncrypter"
      member = "serviceAccount:export@example-edp-project.iam.gserviceaccount.com"
    }
  }
}

run "already_labeled_permissions" {
  module {
    source = "../../../../main/terraform/gcloud/modules/edp-aggregator-edp-deployment"
  }
  command = plan
  assert {
    condition     = google_kms_crypto_key_iam_member.edp_sa_decrypter.role == "roles/cloudkms.cryptoKeyDecrypter" && google_kms_crypto_key_iam_member.additional_access["export"].role == "roles/cloudkms.cryptoKeyEncrypter"
    error_message = "Already-labeled workloads need decrypt only; exports need encrypt only."
  }
  assert {
    condition     = google_kms_key_ring.edp_key_ring.location == "us-central1" && output.kms_audience == "//iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/halo-workloads/providers/confidential-space"
    error_message = "Regional KMS must work with globally scoped federation."
  }
  assert {
    condition     = output.kek_uri == "gcp-kms://projects/example-edp-project/locations/us-central1/keyRings/edp-key-ring/cryptoKeys/edp-kek"
    error_message = "The onboarding output must include the complete Tink key URI."
  }
}

run "parquet_permissions" {
  module {
    source = "../../../../main/terraform/gcloud/modules/edp-aggregator-edp-deployment"
  }
  command = plan
  variables {
    attested_kms_role = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  }
  assert {
    condition     = google_kms_crypto_key_iam_member.edp_sa_decrypter.role == "roles/cloudkms.cryptoKeyEncrypterDecrypter"
    error_message = "Parquet labeling must be able to encrypt labeled output as well as decrypt input."
  }
}

run "reject_key_administration_as_workload_access" {
  module {
    source = "../../../../main/terraform/gcloud/modules/edp-aggregator-edp-deployment"
  }
  command = plan
  variables {
    attested_kms_role = "roles/cloudkms.admin"
  }
  expect_failures = [var.attested_kms_role]
}
