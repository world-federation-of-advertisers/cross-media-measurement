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

moved {
  from = google_service_account.edp_service_account
  to   = module.workload_identity.google_service_account.attested_workload
}

moved {
  from = google_iam_workload_identity_pool.edp_workload_identity_pool
  to   = module.workload_identity.google_iam_workload_identity_pool.pool
}

moved {
  from = google_service_account_iam_member.edp_sa_workload_identity_user
  to   = module.workload_identity.google_service_account_iam_member.federated_access
}
