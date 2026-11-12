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
mock_provider "aws" {
  override_during = plan
}

variables {
  workloads = {
    "trustee-mill" : {
      "service_account_name" : "tee-trustee-mill",
      "workload_identity_pool_id" : "tee-trustee-mill",
      "aws_role_name" : "tee-trustee-mill",
      "workload_project_ids" : [
        "workload-project"
      ],
      "workload_service_accounts" : [
        "trustee-mill-mig-sa@workload-project.iam.gserviceaccount.com"
      ],
      "image_repositories" : [
        "ghcr.io/world-federation-of-advertisers/duchy/trus-tee-mill"
      ],
      "image_signature_fingerprints" : [
        "ECDSA_P256_SHA256:158c62a587e424c0346b9b397b83f51422fa4b403e86c49f4d84ec5d53742eb1"
      ],
      "google_key_permissions" : {
        "trustee_frequency_vector" : [
          "roles/cloudkms.cryptoKeyDecrypter"
        ]
      },
      "aws_key_permissions" : {
        "trustee_frequency_vector" : [
          "kms:Decrypt"
        ]
      }
    },
    "results-fulfiller" : {
      "service_account_name" : "tee-results-fulfiller",
      "workload_identity_pool_id" : "tee-results-fulfiller",
      "aws_role_name" : "tee-results-fulfiller",
      "workload_project_ids" : [
        "workload-project"
      ],
      "workload_service_accounts" : [
        "results-fulfiller-sa@workload-project.iam.gserviceaccount.com"
      ],
      "image_repositories" : [
        "ghcr.io/world-federation-of-advertisers/edp-aggregator/results_fulfiller"
      ],
      "image_signature_fingerprints" : [
        "ECDSA_P256_SHA256:accb7b441ff69e010e3d17278ed03ddeab485167467ffbbbcaeb5a8220bde26f"
      ],
      "google_key_permissions" : {
        "raw_input" : [
          "roles/cloudkms.cryptoKeyDecrypter"
        ],
        "labeled_impressions" : [
          "roles/cloudkms.cryptoKeyDecrypter"
        ],
        "trustee_frequency_vector" : [
          "roles/cloudkms.cryptoKeyEncrypter"
        ]
      },
      "aws_key_permissions" : {
        "raw_input" : [
          "kms:Decrypt"
        ],
        "labeled_impressions" : [
          "kms:Decrypt"
        ],
        "trustee_frequency_vector" : [
          "kms:Encrypt"
        ]
      }
    },
    "subpool-assigner" : {
      "service_account_name" : "tee-subpool-assigner",
      "workload_identity_pool_id" : "tee-subpool-assigner",
      "aws_role_name" : "tee-subpool-assigner",
      "workload_project_ids" : [
        "workload-project"
      ],
      "workload_service_accounts" : [
        "subpool-assigner-sa@workload-project.iam.gserviceaccount.com"
      ],
      "image_repositories" : [
        "ghcr.io/world-federation-of-advertisers/edp-aggregator/subpool_assigner"
      ],
      "image_signature_fingerprints" : [
        "ECDSA_P256_SHA256:e747258eda994a98c1932984162c008c801fa74ee72a714d3d25378c8a2a2362"
      ],
      "google_key_permissions" : {
        "subpool_map" : [
          "roles/cloudkms.cryptoKeyEncrypter"
        ]
      },
      "aws_key_permissions" : {
        "subpool_map" : [
          "kms:Encrypt"
        ]
      }
    },
    "vid-rank-builder" : {
      "service_account_name" : "tee-vid-rank-builder",
      "workload_identity_pool_id" : "tee-vid-rank-builder",
      "aws_role_name" : "tee-vid-rank-builder",
      "workload_project_ids" : [
        "workload-project"
      ],
      "workload_service_accounts" : [
        "vid-rank-builder-sa@workload-project.iam.gserviceaccount.com"
      ],
      "image_repositories" : [
        "ghcr.io/world-federation-of-advertisers/edp-aggregator/vid_rank_builder"
      ],
      "image_signature_fingerprints" : [
        "ECDSA_P256_SHA256:7fa2a3cd9ddf7118c9144c727b27936bbfdf439d569af9f9f67b65183d98aee1"
      ],
      "google_key_permissions" : {
        "raw_input" : [
          "roles/cloudkms.cryptoKeyDecrypter"
        ],
        "subpool_map" : [
          "roles/cloudkms.cryptoKeyDecrypter"
        ],
        "rank_index" : [
          "roles/cloudkms.cryptoKeyEncrypter",
          "roles/cloudkms.cryptoKeyDecrypter"
        ]
      },
      "aws_key_permissions" : {
        "raw_input" : [
          "kms:Decrypt"
        ],
        "subpool_map" : [
          "kms:Decrypt"
        ],
        "rank_index" : [
          "kms:Encrypt",
          "kms:Decrypt"
        ]
      }
    },
    "vid-labeler" : {
      "service_account_name" : "tee-vid-labeler",
      "workload_identity_pool_id" : "tee-vid-labeler",
      "aws_role_name" : "tee-vid-labeler",
      "workload_project_ids" : [
        "workload-project"
      ],
      "workload_service_accounts" : [
        "vid-labeler-sa@workload-project.iam.gserviceaccount.com"
      ],
      "image_repositories" : [
        "ghcr.io/world-federation-of-advertisers/edp-aggregator/vid_labeler"
      ],
      "image_signature_fingerprints" : [
        "ECDSA_P256_SHA256:efac56d8ea536d02c13464b9059a9ffbd6450bc49c7f3cff5ad554b6fd060219"
      ],
      "google_key_permissions" : {
        "raw_input" : [
          "roles/cloudkms.cryptoKeyDecrypter"
        ],
        "rank_index" : [
          "roles/cloudkms.cryptoKeyDecrypter"
        ],
        "labeled_impressions" : [
          "roles/cloudkms.cryptoKeyEncrypter"
        ]
      },
      "aws_key_permissions" : {
        "raw_input" : [
          "kms:Decrypt"
        ],
        "rank_index" : [
          "kms:Decrypt"
        ],
        "labeled_impressions" : [
          "kms:Encrypt"
        ]
      }
    }
  }
  aws_account_id = "123456789012"
  key_administrator_arns = [
    "arn:aws:iam::123456789012:root"
  ]
  google_keys = {
    "raw_input" : {
      "key_ring_id" : "projects/key-project/locations/global/keyRings/edp",
      "key_name" : "raw_input",
      "existing_key_id" : null, "rotation_period" : null
    },
    "subpool_map" : {
      "key_ring_id" : "projects/key-project/locations/global/keyRings/edp",
      "key_name" : "subpool_map",
      "existing_key_id" : null, "rotation_period" : null
    },
    "rank_index" : {
      "key_ring_id" : "projects/key-project/locations/global/keyRings/edp",
      "key_name" : "rank_index",
      "existing_key_id" : null, "rotation_period" : null
    },
    "labeled_impressions" : {
      "key_ring_id" : "projects/key-project/locations/global/keyRings/edp",
      "key_name" : "labeled_impressions",
      "existing_key_id" : null, "rotation_period" : null
    },
    "trustee_frequency_vector" : {
      "key_ring_id" : "projects/key-project/locations/global/keyRings/edp",
      "key_name" : "trustee_frequency_vector",
      "existing_key_id" : null, "rotation_period" : null
    }
  }
  aws_keys = {
    "raw_input" : {
      "alias" : "alias/raw_input",
      "description" : "raw_input",
      "existing_key_arn" : null, "enable_key_rotation" : true, "deletion_window_in_days" : 30
    },
    "subpool_map" : {
      "alias" : "alias/subpool_map",
      "description" : "subpool_map",
      "existing_key_arn" : null, "enable_key_rotation" : true, "deletion_window_in_days" : 30
    },
    "rank_index" : {
      "alias" : "alias/rank_index",
      "description" : "rank_index",
      "existing_key_arn" : null, "enable_key_rotation" : true, "deletion_window_in_days" : 30
    },
    "labeled_impressions" : {
      "alias" : "alias/labeled_impressions",
      "description" : "labeled_impressions",
      "existing_key_arn" : null, "enable_key_rotation" : true, "deletion_window_in_days" : 30
    },
    "trustee_frequency_vector" : {
      "alias" : "alias/trustee_frequency_vector",
      "description" : "trustee_frequency_vector",
      "existing_key_arn" : null, "enable_key_rotation" : true, "deletion_window_in_days" : 30
    }
  }
}

run "isolate_five_jobs_and_five_purposes" {
  command = plan
  module { source = "../../../../../src/main/terraform/gcloud/modules/tee-workload-kms" }
  override_resource {
    target          = module.identity["trustee-mill"].google_service_account.attested_workload
    override_during = plan
    values = {
      email     = "tee-trustee-mill@key-project.iam.gserviceaccount.com"
      name      = "projects/key-project/serviceAccounts/tee-trustee-mill@key-project.iam.gserviceaccount.com"
      unique_id = "100000000000000000000"
    }
  }
  override_resource {
    target          = module.identity["results-fulfiller"].google_service_account.attested_workload
    override_during = plan
    values = {
      email     = "tee-results-fulfiller@key-project.iam.gserviceaccount.com"
      name      = "projects/key-project/serviceAccounts/tee-results-fulfiller@key-project.iam.gserviceaccount.com"
      unique_id = "100000000000000000001"
    }
  }
  override_resource {
    target          = module.identity["subpool-assigner"].google_service_account.attested_workload
    override_during = plan
    values = {
      email     = "tee-subpool-assigner@key-project.iam.gserviceaccount.com"
      name      = "projects/key-project/serviceAccounts/tee-subpool-assigner@key-project.iam.gserviceaccount.com"
      unique_id = "100000000000000000002"
    }
  }
  override_resource {
    target          = module.identity["vid-rank-builder"].google_service_account.attested_workload
    override_during = plan
    values = {
      email     = "tee-vid-rank-builder@key-project.iam.gserviceaccount.com"
      name      = "projects/key-project/serviceAccounts/tee-vid-rank-builder@key-project.iam.gserviceaccount.com"
      unique_id = "100000000000000000003"
    }
  }
  override_resource {
    target          = module.identity["vid-labeler"].google_service_account.attested_workload
    override_during = plan
    values = {
      email     = "tee-vid-labeler@key-project.iam.gserviceaccount.com"
      name      = "projects/key-project/serviceAccounts/tee-vid-labeler@key-project.iam.gserviceaccount.com"
      unique_id = "100000000000000000004"
    }
  }
  override_resource {
    target          = aws_kms_key.purpose["raw_input"]
    override_during = plan
    values          = { arn = "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000001" }
  }
  override_resource {
    target          = aws_kms_key.purpose["subpool_map"]
    override_during = plan
    values          = { arn = "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000002" }
  }
  override_resource {
    target          = aws_kms_key.purpose["rank_index"]
    override_during = plan
    values          = { arn = "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000003" }
  }
  override_resource {
    target          = aws_kms_key.purpose["labeled_impressions"]
    override_during = plan
    values          = { arn = "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000004" }
  }
  override_resource {
    target          = aws_kms_key.purpose["trustee_frequency_vector"]
    override_during = plan
    values          = { arn = "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000005" }
  }
  override_resource {
    target          = google_kms_crypto_key.purpose["raw_input"]
    override_during = plan
    values          = { id = "projects/key-project/locations/global/keyRings/edp/cryptoKeys/raw_input" }
  }
  override_resource {
    target          = google_kms_crypto_key.purpose["subpool_map"]
    override_during = plan
    values          = { id = "projects/key-project/locations/global/keyRings/edp/cryptoKeys/subpool_map" }
  }
  override_resource {
    target          = google_kms_crypto_key.purpose["rank_index"]
    override_during = plan
    values          = { id = "projects/key-project/locations/global/keyRings/edp/cryptoKeys/rank_index" }
  }
  override_resource {
    target          = google_kms_crypto_key.purpose["labeled_impressions"]
    override_during = plan
    values          = { id = "projects/key-project/locations/global/keyRings/edp/cryptoKeys/labeled_impressions" }
  }
  override_resource {
    target          = google_kms_crypto_key.purpose["trustee_frequency_vector"]
    override_during = plan
    values          = { id = "projects/key-project/locations/global/keyRings/edp/cryptoKeys/trustee_frequency_vector" }
  }
  assert {
    condition     = length(output.workloads) == 5 && length(output.google_kek_uris) == 5 && length(output.aws_kek_uris) == 5
    error_message = "Provision every TEE job and purpose in both clouds."
  }
  assert {
    condition     = alltrue([for condition in values(output.attribute_conditions) : strcontains(condition, "repo + ':'") && strcontains(condition, "repo + '@'") && strcontains(condition, "CONFIDENTIAL_SPACE") && strcontains(condition, "image_signatures")])
    error_message = "Require signed, stable Confidential Space and a full repository boundary."
  }
  assert {
    condition     = !strcontains(output.attribute_conditions["trustee-mill"], "results_fulfiller") && !strcontains(output.attribute_conditions["trustee-mill"], "results-fulfiller-sa")
    error_message = "A Mill identity must not admit Results Fulfiller."
  }
  assert {
    condition     = jsondecode(aws_iam_role.workload["trustee-mill"].assume_role_policy).Statement[0].Principal.Federated == "accounts.google.com"
    error_message = "AWS must use Google attestation admission, which checks image_reference."
  }
  assert {
    condition     = toset(jsondecode(aws_iam_role_policy.workload["trustee-mill"].policy).Statement[0].Action) == toset(["kms:Decrypt"]) && jsondecode(aws_iam_role_policy.workload["trustee-mill"].policy).Statement[0].Resource == aws_kms_key.purpose["trustee_frequency_vector"].arn
    error_message = "The Mill may only decrypt its frequency-vector purpose key."
  }
  assert {
    condition     = length([for grant in google_kms_crypto_key_iam_member.workload : grant if grant.member == "serviceAccount:${module.identity["trustee-mill"].service_account_email}"]) == 1
    error_message = "The Mill must have only one Google KEK grant."
  }

}
run "reject_shared_job_signers" {
  command = plan
  module { source = "../../../../../src/main/terraform/gcloud/modules/tee-workload-kms" }
  variables {
    workloads = { for job, w in var.workloads : job => merge(w, {
      image_signature_fingerprints = ["ECDSA_P256_SHA256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
    }) }
  }
  expect_failures = [var.workloads]
}
run "reject_tagged_repository_allowlist" {
  command = plan
  module { source = "../../../../../src/main/terraform/gcloud/modules/tee-workload-kms" }
  variables {
    workloads = { for job, w in var.workloads : job => merge(w, {
      image_repositories = [for repo in w.image_repositories : "${repo}:v1"]
    }) }
  }
  expect_failures = [var.workloads]
}
