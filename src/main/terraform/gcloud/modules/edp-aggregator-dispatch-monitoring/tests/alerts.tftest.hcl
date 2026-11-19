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

mock_provider "google" {}

variables {
  project_id                  = "test-project"
  dead_letter_subscription_id = "vid-labeling-dispatch-queue-dlq-sub"
}

run "preparation_counters_and_policies" {
  command = plan

  assert {
    condition = (
      google_logging_metric.dispatch["failed"].filter == "resource.type=\"cloud_run_revision\" AND (textPayload=~\"(^|[[:space:]])event=edpa[.]vid_labeling[.]dispatch[.]work_item([[:space:]]|$)\" OR jsonPayload.message=~\"(^|[[:space:]])event=edpa[.]vid_labeling[.]dispatch[.]work_item([[:space:]]|$)\") AND (textPayload=~\"(^|[[:space:]])xmm[.]outcome=failed([[:space:]]|$)\" OR jsonPayload.message=~\"(^|[[:space:]])xmm[.]outcome=failed([[:space:]]|$)\")" &&
      google_logging_metric.dispatch["retry_exhausted"].filter == "resource.type=\"k8s_container\" AND (textPayload=~\"(^|[[:space:]])event=secure_computation[.]work_item[.]retry_exhausted([[:space:]]|$)\" OR jsonPayload.message=~\"(^|[[:space:]])event=secure_computation[.]work_item[.]retry_exhausted([[:space:]]|$)\") AND (textPayload=~\"(^|[[:space:]])xmm[.]pubsub[.]subscription[.]id=vid-labeling-dispatch-queue-dlq-sub([[:space:]]|$)\" OR jsonPayload.message=~\"(^|[[:space:]])xmm[.]pubsub[.]subscription[.]id=vid-labeling-dispatch-queue-dlq-sub([[:space:]]|$)\") AND (textPayload=~\"(^|[[:space:]])xmm[.]outcome=retry_exhausted([[:space:]]|$)\" OR jsonPayload.message=~\"(^|[[:space:]])xmm[.]outcome=retry_exhausted([[:space:]]|$)\")"
    )
    error_message = "Rendered metric filters must require the exact resource guard AND every event, outcome and subscription condition."
  }

  assert {
    condition = (
      toset(keys(google_logging_metric.dispatch)) == toset(["failed", "retry_exhausted"]) &&
      toset(keys(google_monitoring_alert_policy.dispatch)) == toset(["failed", "retry_exhausted"]) &&
      alltrue([for counter in google_logging_metric.dispatch :
        counter.metric_descriptor[0].metric_kind == "DELTA" &&
        counter.metric_descriptor[0].value_type == "INT64" &&
        counter.metric_descriptor[0].unit == "1" &&
        length(counter.metric_descriptor[0].labels) == 0
      ])
    )
    error_message = "Both outcome counters must be DELTA integers without per-task metric labels."
  }

  assert {
    condition = (
      strcontains(google_logging_metric.dispatch["failed"].filter, "event=edpa[.]vid_labeling[.]dispatch[.]work_item([[:space:]]|$)") &&
      strcontains(google_logging_metric.dispatch["failed"].filter, "xmm[.]outcome=failed([[:space:]]|$)") &&
      strcontains(google_logging_metric.dispatch["failed"].filter, "jsonPayload.message") &&
      strcontains(google_logging_metric.dispatch["retry_exhausted"].filter, "event=secure_computation[.]work_item[.]retry_exhausted([[:space:]]|$)") &&
      strcontains(google_logging_metric.dispatch["retry_exhausted"].filter, "xmm[.]pubsub[.]subscription[.]id=vid-labeling-dispatch-queue-dlq-sub([[:space:]]|$)") &&
      strcontains(google_logging_metric.dispatch["retry_exhausted"].filter, "xmm[.]outcome=retry_exhausted([[:space:]]|$)")
    )
    error_message = "Filters must match actual payload-free events, outcome boundaries and the exact listener subscription."
  }

  assert {
    condition = alltrue([
      for name, policy in google_monitoring_alert_policy.dispatch :
      policy.conditions[0].condition_threshold[0].filter == "metric.type=\"logging.googleapis.com/user/edpa_vid_dispatch_${name}\" AND resource.type=\"${local.signals[name].resource_type}\"" &&
      policy.conditions[0].condition_threshold[0].threshold_value == 0 &&
      policy.conditions[0].condition_threshold[0].comparison == "COMPARISON_GT" &&
      policy.conditions[0].condition_threshold[0].duration == "0s" &&
      policy.conditions[0].condition_threshold[0].aggregations[0].per_series_aligner == "ALIGN_SUM" &&
      policy.conditions[0].condition_threshold[0].aggregations[0].cross_series_reducer == "REDUCE_SUM" &&
      length(policy.notification_channels) == 0 &&
      strcontains(policy.documentation[0].content, "#4912")
    ])
    error_message = "Alerts must count events across instances and describe the explicitly deferred channel readiness."
  }
}

run "configured_channels_and_exact_subscription" {
  command = plan

  variables {
    dead_letter_subscription_id = "dispatch.special+queue-dlq-sub"
    notification_channels       = ["projects/test-project/notificationChannels/operator"]
  }

  assert {
    condition = (
      strcontains(google_logging_metric.dispatch["retry_exhausted"].filter, "id=dispatch[.]special[+]queue-dlq-sub([[:space:]]|$)") &&
      alltrue([for policy in google_monitoring_alert_policy.dispatch :
        toset(policy.notification_channels) == toset(["projects/test-project/notificationChannels/operator"])
      ])
    )
    error_message = "Configured routes must attach to both policies and subscription IDs must be literal regex matches."
  }
}

run "reject_full_subscription_path" {
  command = plan
  variables {
    dead_letter_subscription_id = "projects/test-project/subscriptions/dispatch-dlq-sub"
  }
  expect_failures = [var.dead_letter_subscription_id]
}


run "log_boundaries_and_non_failure_outcomes" {
  command = plan

  assert {
    condition = (
      alltrue([for pattern in local.message_patterns["failed"] :
        length(regexall(pattern, "WARNING: event=edpa.vid_labeling.dispatch.work_item xmm.work_item.name=workItems/task xmm.outcome=failed\n")) > 0
      ]) &&
      alltrue([for pattern in local.message_patterns["retry_exhausted"] :
        length(regexall(pattern, "event=secure_computation.work_item.retry_exhausted xmm.pubsub.subscription.id=vid-labeling-dispatch-queue-dlq-sub xmm.work_item.name=workItems/task xmm.work_item.generation=7 xmm.outcome=retry_exhausted\n")) > 0
      ]) &&
      !alltrue([for pattern in local.message_patterns["failed"] :
        length(regexall(pattern, "event=edpa.vid_labeling.dispatch.work_item xmm.outcome=retryable_failure")) > 0
      ]) &&
      !alltrue([for pattern in local.message_patterns["failed"] :
        length(regexall(pattern, "event=edpa.vid_labeling.dispatch.work_item xmm.outcome=failed_other")) > 0
      ]) &&
      !alltrue([for pattern in local.message_patterns["retry_exhausted"] :
        length(regexall(pattern, "event=secure_computation.work_item.retry_exhausted xmm.pubsub.subscription.id=vid-labeling-dispatch-queue-dlq-sub-other xmm.outcome=retry_exhausted")) > 0
      ])
    )
    error_message = "Actual metric patterns must accept JUL/newline records and reject transient, prefix and other-subscription events."
  }
}
