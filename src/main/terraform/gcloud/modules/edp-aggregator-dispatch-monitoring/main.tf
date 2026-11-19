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
  subscription_pattern = replace(replace(var.dead_letter_subscription_id, ".", "[.]"), "+", "[+]")
  message_patterns = {
    failed = [
      "(^|[[:space:]])event=edpa[.]vid_labeling[.]dispatch[.]work_item([[:space:]]|$)",
      "(^|[[:space:]])xmm[.]outcome=failed([[:space:]]|$)",
    ]
    retry_exhausted = [
      "(^|[[:space:]])event=secure_computation[.]work_item[.]retry_exhausted([[:space:]]|$)",
      "(^|[[:space:]])xmm[.]pubsub[.]subscription[.]id=${local.subscription_pattern}([[:space:]]|$)",
      "(^|[[:space:]])xmm[.]outcome=retry_exhausted([[:space:]]|$)",
    ]
  }
  signals = {
    failed = {
      filter = join(" AND ", concat(
        ["resource.type=\"cloud_run_revision\""],
        [for pattern in local.message_patterns["failed"] :
          "(textPayload=~${jsonencode(pattern)} OR jsonPayload.message=~${jsonencode(pattern)})"
        ],
      ))
      resource_type = "cloud_run_revision"
      action        = "A VID dispatch WorkItem became FAILED. Inspect the dispatch lifecycle logs for its WorkItem name, execution generation, attempt and safe error classification. The upload may not yet exist."
    }
    retry_exhausted = {
      filter = join(" AND ", concat(
        ["resource.type=\"k8s_container\""],
        [for pattern in local.message_patterns["retry_exhausted"] :
          "(textPayload=~${jsonencode(pattern)} OR jsonPayload.message=~${jsonencode(pattern)})"
        ],
      ))
      resource_type = "k8s_container"
      action        = "VID dispatch deliveries exhausted Pub/Sub retries and the generic listener recorded FAILED state. Inspect the exhaustion event for the WorkItem name and execution generation. An empty DLQ does not clear this failure."
    }
  }
}

resource "google_logging_metric" "dispatch" {
  for_each    = local.signals
  project     = var.project_id
  name        = "edpa_vid_dispatch_${each.key}"
  description = each.value.action
  filter      = each.value.filter

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }
}

resource "google_monitoring_alert_policy" "dispatch" {
  for_each              = local.signals
  project               = var.project_id
  depends_on            = [google_logging_metric.dispatch]
  display_name          = "EDPA VID dispatch ${replace(each.key, "_", " ")}"
  combiner              = "OR"
  notification_channels = var.notification_channels

  conditions {
    display_name = each.key
    condition_threshold {
      filter          = "metric.type=\"logging.googleapis.com/user/edpa_vid_dispatch_${each.key}\" AND resource.type=\"${each.value.resource_type}\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"
      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_SUM"
        cross_series_reducer = "REDUCE_SUM"
      }
    }
  }

  alert_strategy {
    auto_close = "1800s"
  }

  documentation {
    content = "${each.value.action} Correct the underlying configuration/input before RetryWorkItem, using the execution generation inspected. Individual task IDs stay in payload-free logs, not metric labels. Notification routing and verified dispatch cutover readiness are tracked in cross-media-measurement#4912; empty channels create incidents without notifying an operator."
  }
}
