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
}
variables {
  workload_service_account_names = ["trustee-mill-sa", "results-fulfiller-sa"]
  workloads                      = { "dev" : { "project_id" : "example-dev", "provider_id" : "cs-dev", "image_signature_fingerprints" : ["ECDSA_P256_SHA256:0000000000000000000000000000000000000000000000000000000000000000"] }, "head" : { "project_id" : "example-head", "provider_id" : "cs-head", "image_signature_fingerprints" : ["ECDSA_P256_SHA256:0000000000000000000000000000000000000000000000000000000000000000"] }, "qa" : { "project_id" : "example-qa", "provider_id" : "cs-qa", "image_signature_fingerprints" : ["ECDSA_P256_SHA256:1111111111111111111111111111111111111111111111111111111111111111"] } }
}
run "signed_workload_policies" {
  module { source = "../../../../main/terraform/gcloud/modules/edp-kms-attestation-policy" }
  command = plan
  assert {
    condition = alltrue([for env, policy in output.attribute_conditions :
      strcontains(policy, "assertion.swname == 'CONFIDENTIAL_SPACE'") &&
      strcontains(policy, "'STABLE' in assertion.submods.confidential_space.support_attributes") &&
      strcontains(policy, "assertion.submods.container.image_signatures") &&
      strcontains(policy, "assertion.google_service_accounts") &&
      strcontains(policy, var.workloads[env].project_id) &&
      strcontains(policy, one(var.workloads[env].image_signature_fingerprints))
    ])
    error_message = "Every environment must require Confidential Space, its project and accounts, and its approved signer."
  }

}
run "reject_unsigned_environment" {
  module { source = "../../../../main/terraform/gcloud/modules/edp-kms-attestation-policy" }
  command = plan
  variables {
    workloads = {
      dev  = { project_id = "example-dev", provider_id = "cs-dev", image_signature_fingerprints = [] }
      head = { project_id = "example-head", provider_id = "cs-head", image_signature_fingerprints = [] }
      qa   = { project_id = "example-qa", provider_id = "cs-qa", image_signature_fingerprints = [] }
    }
  }
  expect_failures = [var.workloads]
}
run "reject_encoded_public_key" {
  module { source = "../../../../main/terraform/gcloud/modules/edp-kms-attestation-policy" }
  command = plan
  variables {
    workloads = {
      dev  = { project_id = "example-dev", provider_id = "cs-dev", image_signature_fingerprints = ["ECDSA_P256_SHA256:BASE64_PEM"] }
      head = { project_id = "example-head", provider_id = "cs-head", image_signature_fingerprints = [] }
      qa   = { project_id = "example-qa", provider_id = "cs-qa", image_signature_fingerprints = [] }
    }
  }
  expect_failures = [var.workloads]
}
