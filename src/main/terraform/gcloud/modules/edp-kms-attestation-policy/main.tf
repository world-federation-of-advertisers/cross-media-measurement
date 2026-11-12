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

locals {
  attribute_conditions = {
    for env, workload in var.workloads : env => join(" && ", [
      "assertion.swname == 'CONFIDENTIAL_SPACE'",
      "'STABLE' in assertion.submods.confidential_space.support_attributes",
      "assertion.submods.gce.project_id == ${jsonencode(workload.project_id)}",
      "${jsonencode([for name in var.workload_service_account_names : "${name}@${workload.project_id}.iam.gserviceaccount.com"])}.exists(sa, sa in assertion.google_service_accounts)",
      "${jsonencode(workload.image_signature_fingerprints)}.exists(fingerprint, fingerprint in assertion.submods.container.image_signatures.map(sig, sig.signature_algorithm + ':' + sig.key_id))",
    ])
  }
}
output "attribute_conditions" {
  value = local.attribute_conditions
}
