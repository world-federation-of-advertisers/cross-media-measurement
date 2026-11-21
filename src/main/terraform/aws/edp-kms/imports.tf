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
  to = module.edp_kms.aws_kms_key.kek
  id = local.key_id
}
import {
  to = module.edp_kms.aws_kms_alias.kek
  id = var.key_alias
}
import {
  to = module.edp_kms.aws_iam_role.attested_workload
  id = var.role_name
}
import {
  to = aws_iam_openid_connect_provider.attestation
  id = "arn:aws:iam::${local.account_id}:oidc-provider/confidentialcomputing.googleapis.com"
}
import {
  to = aws_iam_role_policy.attested_workload
  id = "${var.role_name}:Policy"
}
import {
  to = aws_iam_role.export
  id = var.export_role_name
}
import {
  to = aws_iam_role_policy.export
  id = "${var.export_role_name}:KmsAccess"
}
