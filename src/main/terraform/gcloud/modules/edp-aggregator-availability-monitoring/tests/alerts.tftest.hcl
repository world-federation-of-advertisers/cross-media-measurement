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
      google_monitoring_alert_policy.availability["pending_availability_age"].conditions[0].condition_threshold[0].threshold_value == 86400 &&
      google_monitoring_alert_policy.availability["failed_availability_executions"].conditions[0].condition_threshold[0].threshold_value == 3 &&
      google_monitoring_alert_policy.availability["terminal_availability_failures"].conditions[0].condition_threshold[0].threshold_value == 1 &&
      google_monitoring_alert_policy.availability["availability_publication_age"].conditions[0].condition_threshold[0].threshold_value == 3600
    )
    error_message = "All requested availability conditions must use the documented defaults."
  }

  assert {
    condition = alltrue([
      for name, policy in google_monitoring_alert_policy.availability :
      policy.conditions[0].condition_threshold[0].filter == "metric.type=\"workload.googleapis.com/edpa.vid_labeling_monitor.${name}\" AND resource.type=\"generic_task\"" &&
      policy.conditions[0].condition_threshold[0].aggregations[0].per_series_aligner == "ALIGN_MAX" &&
      policy.conditions[0].condition_threshold[0].duration == "0s" &&
      google_monitoring_metric_descriptor.availability[name].metric_kind == "GAUGE"
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
      google_monitoring_alert_policy.availability["pending_availability_age"].conditions[0].condition_threshold[0].threshold_value == 7200 &&
      google_monitoring_alert_policy.availability["failed_availability_executions"].conditions[0].condition_threshold[0].threshold_value == 5 &&
      google_monitoring_alert_policy.availability["availability_publication_age"].conditions[0].condition_threshold[0].threshold_value == 1800
    )
    error_message = "Configured thresholds must reach every policy."
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
