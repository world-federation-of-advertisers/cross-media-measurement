# Copyright 2023 The Cross-Media Measurement Authors
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

variable "name" {
  description = "Name of the storage bucket."
  type        = string
  nullable    = false
}

variable "location" {
  description = "Location of the storage bucket."
  type        = string
  nullable    = false
}

variable "lifecycle_rules" {
  description = <<-EOT
    Lifecycle rule configurations. Each entry can target a specific prefix (e.g., for per-EDP
    folders) or the entire bucket (empty prefix).
    
    Each entry contains:
    - name: Identifier for the rule (used for documentation/clarity)
    - prefix: Object prefix to match (e.g., "edp/edp7/" for a folder, or "" for entire bucket)
    - retention_days: Days to retain objects after Custom-Time (e.g., impression date)
    - enable_fallback: Whether to enable fallback rule based on upload date (default: true)
    - fallback_retention_days: Days to retain objects after upload (default: 90)
  EOT
  type = list(object({
    name                    = string
    prefix                  = string
    retention_days          = number
    enable_fallback         = optional(bool, true)
    fallback_retention_days = optional(number, 1460)  # Default: 4 years
  }))
  default = []
}
