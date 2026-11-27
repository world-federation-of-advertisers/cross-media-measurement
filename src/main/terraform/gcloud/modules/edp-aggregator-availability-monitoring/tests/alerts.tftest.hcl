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

run "defaults_without_notifications" {
  command = plan

  assert {
    condition = alltrue([
      for policy in google_monitoring_alert_policy.availability :
      length(policy.notification_channels) == 0
    ])
    error_message = "Default policies must allow empty notification channels."
  }

  assert {
    condition = (
      google_monitoring_alert_policy.availability["pending_availability_age"].conditions[0].condition_threshold[0].threshold_value == 86399 &&
      google_monitoring_alert_policy.availability["failed_availability_executions"].conditions[0].condition_threshold[0].threshold_value == 2 &&
      google_monitoring_alert_policy.availability["terminal_availability_failures"].conditions[0].condition_threshold[0].threshold_value == 0 &&
      google_monitoring_alert_policy.availability["availability_publication_age"].conditions[0].condition_threshold[0].threshold_value == 3599 &&
      google_monitoring_alert_policy.availability["availability_monitoring_errors"].conditions[0].condition_threshold[0].threshold_value == 0
    )
    error_message = "Strict cutoffs must preserve the documented inclusive defaults."
  }

  assert {
    condition = alltrue([
      for name, policy in google_monitoring_alert_policy.availability :
      policy.conditions[0].condition_threshold[0].filter == "metric.type=\"workload.googleapis.com/edpa.vid_labeling_monitor.${name}\" AND resource.type=\"generic_task\"" &&
      policy.conditions[0].condition_threshold[0].aggregations[0].per_series_aligner == "ALIGN_MAX" &&
      policy.conditions[0].condition_threshold[0].duration == "0s" &&
      policy.conditions[0].condition_threshold[0].comparison == "COMPARISON_GT" &&
      google_monitoring_metric_descriptor.availability[name].metric_kind == "GAUGE" &&
      google_monitoring_metric_descriptor.availability[name].value_type == "INT64"
    ])
    error_message = "Policies must evaluate a single scheduled observation using the actual gauge names."
  }
}

run "configured_route_and_thresholds" {
  command = plan
  variables {
    notification_channels = ["projects/test-project/notificationChannels/operator"]
    thresholds = {
      pending_age_seconds     = 7200
      failed_executions       = 5
      publication_age_seconds = 1800
    }
  }

  assert {
    condition = alltrue([
      for policy in google_monitoring_alert_policy.availability :
      toset(policy.notification_channels) == toset(["projects/test-project/notificationChannels/operator"])
    ])
    error_message = "Every availability alert must use the configured operator notification route."
  }

  assert {
    condition = (
      google_monitoring_alert_policy.availability["pending_availability_age"].conditions[0].condition_threshold[0].threshold_value == 7199 &&
      google_monitoring_alert_policy.availability["failed_availability_executions"].conditions[0].condition_threshold[0].threshold_value == 4 &&
      google_monitoring_alert_policy.availability["availability_publication_age"].conditions[0].condition_threshold[0].threshold_value == 1799
    )
    error_message = "Strict cutoffs must preserve the configured inclusive thresholds."
  }
}

run "fractional_age_and_count_boundaries" {
  command = plan
  variables {
    thresholds = {
      pending_age_seconds     = 7200.25
      failed_executions       = 5
      publication_age_seconds = 1800.5
    }
  }

  assert {
    condition = (
      7201 > google_monitoring_alert_policy.availability["pending_availability_age"].conditions[0].condition_threshold[0].threshold_value &&
      7200 <= google_monitoring_alert_policy.availability["pending_availability_age"].conditions[0].condition_threshold[0].threshold_value &&
      1801 > google_monitoring_alert_policy.availability["availability_publication_age"].conditions[0].condition_threshold[0].threshold_value &&
      1800 <= google_monitoring_alert_policy.availability["availability_publication_age"].conditions[0].condition_threshold[0].threshold_value &&
      5 > google_monitoring_alert_policy.availability["failed_availability_executions"].conditions[0].condition_threshold[0].threshold_value &&
      4 <= google_monitoring_alert_policy.availability["failed_availability_executions"].conditions[0].condition_threshold[0].threshold_value &&
      1 > google_monitoring_alert_policy.availability["terminal_availability_failures"].conditions[0].condition_threshold[0].threshold_value &&
      0 <= google_monitoring_alert_policy.availability["terminal_availability_failures"].conditions[0].condition_threshold[0].threshold_value &&
      1 > google_monitoring_alert_policy.availability["availability_monitoring_errors"].conditions[0].condition_threshold[0].threshold_value &&
      0 <= google_monitoring_alert_policy.availability["availability_monitoring_errors"].conditions[0].condition_threshold[0].threshold_value
    )
    error_message = "Each policy must trigger at its first eligible integer observation, including the first failure."
  }
}

run "reject_invalid_thresholds" {
  command = plan
  variables {
    thresholds = { failed_executions = 0 }
  }
  expect_failures = [var.thresholds]
}

run "additional_source_status_access" {
  command = plan
  variables {
    publication_status_storage_roots = {
      "historical-output" = ["historical"]
      "root-output"       = [""]
    }
    publication_status_writer_member = "serviceAccount:availability@test-project.iam.gserviceaccount.com"
  }

  assert {
    condition = (
      google_storage_bucket_iam_member.publication_status["historical-output"].role == "roles/storage.objectUser" &&
      google_storage_bucket_iam_member.publication_status["historical-output"].member == var.publication_status_writer_member &&
      one(google_storage_bucket_iam_member.publication_status["historical-output"].condition).expression == "resource.name.startsWith(\"projects/_/buckets/historical-output/objects/historical/.availability-publication/\")" &&
      one(google_storage_bucket_iam_member.publication_status["root-output"].condition).expression == "resource.name.startsWith(\"projects/_/buckets/root-output/objects/.availability-publication/\")"
    )
    error_message = "Additional source access must cover only publication status objects and the runtime writer."
  }
}

run "reject_empty_source_prefix_set" {
  command = plan
  variables {
    publication_status_storage_roots = { "historical-output" = [] }
  }
  expect_failures = [var.publication_status_storage_roots]
}
