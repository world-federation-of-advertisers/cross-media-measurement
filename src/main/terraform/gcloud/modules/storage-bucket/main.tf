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

  # Lifecycle Rule 1: Delete objects based on Custom-Time (impression date)
  dynamic "lifecycle_rule" {
    for_each = var.enable_lifecycle_rules ? [1] : []
    content {
      condition {
        days_since_custom_time = var.retention_days
        matches_prefix         = var.lifecycle_prefix != "" ? [var.lifecycle_prefix] : null
      }
      action {
        type = "Delete"
      }
    }
  }

  # Lifecycle Rule 2: Safety net for objects without Custom-Time (based on upload date)
  dynamic "lifecycle_rule" {
    for_each = var.enable_lifecycle_rules && var.enable_fallback_lifecycle_rule ? [1] : []
    content {
      condition {
        age            = var.fallback_retention_days
        matches_prefix = var.lifecycle_prefix != "" ? [var.lifecycle_prefix] : null
      }
      action {
        type = "Delete"
      }
    }
  }
}
