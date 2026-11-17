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
  signals = {
    pending_availability_age = {
      threshold = var.thresholds.pending_age_seconds
      unit      = "s"
      action    = "Labeled dates have awaited availability publication too long. Inspect the pending upload/model line and the missing or unfinalized earlier dates; coordinate with the EDP to restore those inputs."
    }
    failed_availability_executions = {
      threshold = var.thresholds.failed_executions
      unit      = "1"
      action    = "The deterministic DataAvailabilitySync WorkItem has repeatedly failed. Inspect its attempts and execution generation, fix the missing input or publication error, and let the monitor retry the same task."
    }
    terminal_availability_failures = {
      threshold = 1
      unit      = "1"
      action    = "A pending DataAvailabilitySync WorkItem was observed terminally failed. Inspect its latest attempt and the model line's pending dates; repeated scheduling does not prove publication succeeded."
    }
    availability_publication_age = {
      threshold = var.thresholds.publication_age_seconds
      unit      = "s"
      action    = "Availability work is pending and actual Kingdom publication has been silent too long. Inspect the affected model line, gap checks and Kingdom publication errors. Blocked and no-work executions do not refresh this timestamp."
    }
    availability_monitoring_errors = {
      threshold = 1
      unit      = "1"
      action    = "Pending availability could not be fully inspected. Check the VidLabelingMonitor logs, storage access and WorkItems API before trusting the age and failure gauges."
    }
  }
}

resource "google_monitoring_metric_descriptor" "availability" {
  for_each     = local.signals
  type         = "workload.googleapis.com/edpa.vid_labeling_monitor.${each.key}"
  metric_kind  = "GAUGE"
  value_type   = "INT64"
  unit         = each.value.unit
  display_name = "edpa.vid_labeling_monitor.${each.key}"
  description  = each.value.action
  labels { key = "instrumentation_source" }
  labels { key = "service_name" }
  labels { key = "instrumentation_version" }
  labels { key = "edpa_vid_labeling_monitor_data_provider" }
}

resource "google_monitoring_alert_policy" "availability" {
  for_each              = local.signals
  depends_on            = [google_monitoring_metric_descriptor.availability]
  display_name          = "EDPA VID-labeling ${replace(each.key, "_", " ")}"
  combiner              = "OR"
  notification_channels = var.notification_channels

  conditions {
    display_name = each.key
    condition_threshold {
      filter          = "metric.type=\"workload.googleapis.com/edpa.vid_labeling_monitor.${each.key}\" AND resource.type=\"generic_task\""
      comparison      = "COMPARISON_GE"
      threshold_value = each.value.threshold
      duration        = "0s"
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }

  documentation {
    content = "${each.value.action} The incident identifies the DataProvider in the metric labels; inspect its pending model lines and the monitor logs. Notifications require a configured channel; an empty channel list creates incidents without sending notifications."
  }
}

resource "google_storage_bucket_iam_member" "publication_status" {
  for_each = var.publication_status_storage_roots
  bucket   = each.key
  role     = "roles/storage.objectUser"
  member   = var.publication_status_writer_member

  condition {
    title       = "write_publication_status"
    description = "Publication status objects in configured source output roots."
    expression = join(" || ", [
      for prefix in each.value :
      "resource.name.startsWith(${jsonencode("projects/_/buckets/${each.key}/objects/${join("/", compact([trim(prefix, "/"), ".availability-publication"]))}/")})"
    ])
  }

  lifecycle {
    precondition {
      condition     = var.publication_status_writer_member != ""
      error_message = "Additional publication status roots require a runtime writer member."
    }
  }
}
