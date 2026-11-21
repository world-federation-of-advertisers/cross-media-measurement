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

import {
  to = module.edp_kms.google_kms_key_ring.edp_key_ring
  id = "projects/${local.project_id}/locations/${local.key_location}/keyRings/${local.key_ring}"
}
import {
  to = module.edp_kms.google_kms_crypto_key.edp_kek
  id = local.input_key_id
}
import {
  to = module.edp_kms.module.workload_identity.google_service_account.attested_workload
  id = "projects/${local.project_id}/serviceAccounts/${var.service_account_name}@${local.project_id}.iam.gserviceaccount.com"
}
import {
  to = module.edp_kms.module.workload_identity.google_iam_workload_identity_pool.pool
  id = "projects/${var.project_number}/locations/global/workloadIdentityPools/${var.workload_identity_pool_id}"
}
import {
  to = module.edp_kms.module.workload_identity.google_iam_workload_identity_pool_provider.oidc
  id = "${local.provider_prefix}/${var.workloads.dev.provider_id}"
}
import {
  for_each = { for env, workload in var.workloads : env => workload if env != "dev" }
  to       = google_iam_workload_identity_pool_provider.environment[each.key]
  id       = "${local.provider_prefix}/${each.value.provider_id}"
}
import {
  to = module.edp_kms.module.workload_identity.google_service_account_iam_member.federated_access
  id = "projects/${local.project_id}/serviceAccounts/${var.service_account_name}@${local.project_id}.iam.gserviceaccount.com roles/iam.workloadIdentityUser principalSet://iam.googleapis.com/projects/${var.project_number}/locations/global/workloadIdentityPools/${var.workload_identity_pool_id}/*"
}
import {
  to = module.edp_kms.google_kms_crypto_key_iam_member.edp_sa_decrypter
  id = "${local.input_key_id} roles/cloudkms.cryptoKeyDecrypter serviceAccount:${var.service_account_name}@${local.project_id}.iam.gserviceaccount.com"
}
import {
  for_each = merge({
    workload_encrypt = "serviceAccount:${var.service_account_name}@${local.project_id}.iam.gserviceaccount.com roles/cloudkms.cryptoKeyEncrypter"
  }, { for member in var.additional_input_key_members : member => "${member} roles/cloudkms.cryptoKeyEncrypterDecrypter" })
  to = module.edp_kms.google_kms_crypto_key_iam_member.additional_access[each.key]
  id = "${local.input_key_id} ${split(" ", each.value)[1]} ${split(" ", each.value)[0]}"
}
import {
  to = google_kms_crypto_key.trustee
  id = "projects/${local.project_id}/locations/${local.key_location}/keyRings/${local.key_ring}/cryptoKeys/${var.trustee_key_name}"
}
import {
  to = google_kms_crypto_key_iam_member.trustee
  id = "projects/${local.project_id}/locations/${local.key_location}/keyRings/${local.key_ring}/cryptoKeys/${var.trustee_key_name} roles/cloudkms.cryptoKeyEncrypterDecrypter serviceAccount:${var.service_account_name}@${local.project_id}.iam.gserviceaccount.com"
}
