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

output "kek_uri" {
  description = "Tink URI of the EDP-owned AWS KMS key."
  value       = "aws-kms://${aws_kms_key.kek.arn}"
}

output "key_arn" {
  description = "KMS key ARN."
  value       = aws_kms_key.kek.arn
}

output "role_arn" {
  description = "Role assumed by the attested Halo workload."
  value       = aws_iam_role.attested_workload.arn
}

output "oidc_provider_arn" {
  description = "Created or reused OIDC provider ARN."
  value       = local.oidc_provider_arn
}

output "role_name" {
  value = aws_iam_role.attested_workload.name
}
output "assume_role_policy" {
  value = aws_iam_role.attested_workload.assume_role_policy
}
