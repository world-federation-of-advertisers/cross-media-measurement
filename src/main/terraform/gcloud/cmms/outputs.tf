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

output "work_item_tee_mig_names" {
  description = "Names of Managed Instance Groups that consume Secure Computation WorkItems."
  value       = module.edp_aggregator.work_item_tee_mig_names
}

output "report_trace_operator_service_account_email" {
  description = "Email of the read-only report-trace operator service account."
  value       = module.reporting_v2.report_trace_operator_service_account_email
}

output "report_trace_operator_postgres_user" {
  description = "Cloud SQL IAM database username for the report-trace operator."
  value       = module.reporting_v2.report_trace_operator_postgres_user
}

output "vid_labeling_trace_operator_service_account_email" {
  description = "Email of the logs-and-traces-only VID-labeling trace operator service account."
  value       = module.reporting_v2.vid_labeling_trace_operator_service_account_email
}
