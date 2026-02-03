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

resource "google_storage_bucket" "bucket" {
  name     = var.name
  location = var.location

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Delete objects based on Custom-Time (e.g., impression date)
  dynamic "lifecycle_rule" {
    for_each = var.lifecycle_rules
    content {
      condition {
        days_since_custom_time = lifecycle_rule.value.retention_days
        matches_prefix         = [lifecycle_rule.value.prefix]
      }
      action {
        type = "Delete"
      }
    }
  }

  # Fallback: Delete objects based on upload date (safety net for objects without Custom-Time)
  dynamic "lifecycle_rule" {
    for_each = [for rule in var.lifecycle_rules : rule if rule.enable_fallback]
    content {
      condition {
        age            = lifecycle_rule.value.fallback_retention_days
        matches_prefix = [lifecycle_rule.value.prefix]
      }
      action {
        type = "Delete"
      }
    }
  }
}
