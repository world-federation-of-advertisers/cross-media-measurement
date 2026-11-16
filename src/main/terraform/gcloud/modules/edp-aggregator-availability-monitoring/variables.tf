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

variable "thresholds" {
  description = "VID-labeling availability alert thresholds; ages are in seconds."
  type = object({
    pending_age_seconds     = optional(number, 86400)
    failed_executions       = optional(number, 3)
    publication_age_seconds = optional(number, 3600)
  })
  default = {}

  validation {
    condition = (
      var.thresholds.pending_age_seconds > 0 &&
      var.thresholds.publication_age_seconds > 0 &&
      var.thresholds.failed_executions >= 1 &&
      floor(var.thresholds.failed_executions) == var.thresholds.failed_executions
    )
    error_message = "Ages must be positive and failed_executions must be a positive integer."
  }
}

variable "notification_channels" {
  description = "Existing Cloud Monitoring channels for availability alerts; empty sends no notifications."
  type        = list(string)
  default     = []
}

variable "publication_status_storage_roots" {
  description = "Additional GCS buckets mapped to source output-root prefixes for publication status access."
  type        = map(set(string))
  default     = {}

  validation {
    condition = alltrue([
      for bucket, prefixes in var.publication_status_storage_roots :
      bucket != "" && length(prefixes) > 0 &&
      alltrue([for prefix in prefixes : !startswith(prefix, "/")])
    ])
    error_message = "Each bucket needs at least one bucket-relative prefix; use an empty prefix for the bucket root."
  }
}

variable "publication_status_writer_member" {
  description = "DataAvailabilitySync runtime IAM member for additional publication status roots."
  type        = string
  default     = ""
}
