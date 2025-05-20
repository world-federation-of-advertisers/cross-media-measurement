# Copyright 2025 The Cross-Media Measurement Authors
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

output "scheduler_service_account" {
  description = "The service account used by the Cloud Scheduler"
  value       = google_service_account.scheduler_service_account
}

output "scheduler_job" {
  description = "The Cloud Scheduler job"
  value       = google_cloud_scheduler_job.job
}