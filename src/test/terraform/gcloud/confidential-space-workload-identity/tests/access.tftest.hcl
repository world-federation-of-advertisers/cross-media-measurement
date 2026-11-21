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

  federated_service_account_role = "roles/iam.workloadIdentityUser"
}

run "google_kms_federation" {
  module {
    source = "../../../../main/terraform/gcloud/modules/confidential-space-workload-identity"
  }
  command = plan

  assert {
    condition     = google_service_account_iam_member.federated_access.role == "roles/iam.workloadIdentityUser" && length(google_service_account_iam_member.self_id_token) == 0
    error_message = "Google KMS federation must not acquire ID-token creation grants."
  }
  assert {
    condition     = google_service_account_iam_member.federated_access.member == "principalSet://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/halo-workloads/*"
    error_message = "Federation must refer to the EDP's globally scoped pool."
  }
  assert {
    condition     = google_iam_workload_identity_pool_provider.oidc.attribute_condition == var.attribute_condition
    error_message = "The provider must retain the supplied attestation restrictions."
  }
}

run "aws_bridge_federation" {
  module {
    source = "../../../../main/terraform/gcloud/modules/confidential-space-workload-identity"
  }
  command = plan
  variables {
    federated_service_account_role = "roles/iam.serviceAccountTokenCreator"
  }

  assert {
    condition     = google_service_account_iam_member.federated_access.role == "roles/iam.serviceAccountTokenCreator" && length(google_service_account_iam_member.self_id_token) == 1
    error_message = "The AWS bridge needs federated token creation and exactly one self ID-token grant."
  }
  assert {
    condition     = google_service_account_iam_member.self_id_token[0].member == "serviceAccount:halo-kms@example-edp-project.iam.gserviceaccount.com"
    error_message = "The ID-token grant must be restricted to the bridge account itself."
  }
}

run "reject_unrestricted_service_account_role" {
  module {
    source = "../../../../main/terraform/gcloud/modules/confidential-space-workload-identity"
  }
  command = plan
  variables {
    federated_service_account_role = "roles/owner"
  }
  expect_failures = [var.federated_service_account_role]
}

run "reject_empty_attestation_condition" {
  module {
    source = "../../../../main/terraform/gcloud/modules/confidential-space-workload-identity"
  }
  command = plan
  variables {
    attribute_condition = " "
  }
  expect_failures = [var.attribute_condition]
}
