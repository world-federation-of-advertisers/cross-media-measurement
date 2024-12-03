# Copyright 2024 The Cross-Media Measurement Authors
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

data "google_project" "project" {}

resource "google_pubsub_topic" "topic" {
  project = data.google_project.project.name
  name = var.topic_name
}

resource "google_pubsub_subscription" "subscription" {
  name  = var.subscription_name
  topic = google_pubsub_topic.topic.id

  ack_deadline_seconds       = var.ack_deadline_seconds
  retain_acked_messages      = var.retain_acked_messages
  message_retention_duration = var.message_retention_duration
}

resource "google_monitoring_alert_policy" "pubsub_alert" {
  display_name = "Pub/Sub Message Alert"
  combiner     = "OR"

  conditions {
    display_name = "High Number of Undelivered Messages"
    condition_threshold {
      filter          = "resource.type=\"pubsub_subscription\" AND metric.type=\"pubsub.googleapis.com/subscription/num_undelivered_messages\" AND resource.label.subscription_id=\"${google_pubsub_subscription.subscription.name}\""
      comparison      = "COMPARISON_GT"
      threshold_value = var.undelivered_messages_threshold
      duration        = "60s"

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MAX"
        cross_series_reducer = "REDUCE_NONE"
      }
    }
  }

  documentation {
    content  = "Alert when the number of undelivered messages exceeds the threshold."
    mime_type = "text/markdown"
  }
}
