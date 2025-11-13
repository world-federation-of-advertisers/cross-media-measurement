# Copyright 2025 The Cross-Media Measurement Authors
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

# SLOs sourced from edpa_monitoring_plan.md
#  - SLO 2: Data Availability Freshness (99.9%, 28d)
#  - SLO 3: Requisition Fulfillment Timeliness (99.9%, 28d)
#  - SLO 4: Fulfillment Correctness (99.99%, 28d)
#  - SLO 5: Event Group Sync Timeliness (99.9%, 28d)

locals {
  # Custom metric type names. Adjust to match actual emitted metric types.
  metric_data_availability_sync_duration      = "custom.googleapis.com/edpa.data_availability.sync_duration"
  metric_requisition_fulfillment_latency      = "custom.googleapis.com/edpa.results_fulfiller.requisition_fulfillment_latency"
  metric_report_fulfillment_latency           = "custom.googleapis.com/edpa.results_fulfiller.report_fulfillment_latency"
  metric_requisitions_processed               = "custom.googleapis.com/edpa.results_fulfiller.requisitions_processed"
  metric_queue_writes                         = "custom.googleapis.com/edpa.data_watcher.queue_writes"
  metric_event_group_sync_latency             = "custom.googleapis.com/edpa.event_group.sync_latency"
}

# Services for grouping SLOs by component
resource "google_monitoring_custom_service" "data_availability_sync" {
  service_id   = "edpa-data-availability-sync"
  display_name = "EDPA - Data Availability Sync"
}

resource "google_monitoring_custom_service" "results_fulfiller" {
  service_id   = "edpa-results-fulfiller"
  display_name = "EDPA - Results Fulfiller"
}

resource "google_monitoring_custom_service" "event_group_sync" {
  service_id   = "edpa-event-group-sync"
  display_name = "EDPA - Event Group Sync"
}

# SLO 2: Data Availability Freshness
resource "google_monitoring_slo" "data_availability_freshness_99_9_28d" {
  service      = google_monitoring_custom_service.data_availability_sync.name
  slo_id       = "data-availability-freshness-99-9-28d"
  display_name = "Data availability freshness <1h (28d)"
  goal                 = 0.999
  rolling_period_days  = 28

  request_based_sli {
    distribution_cut {
      distribution_filter = join(" ", compact([
        format("metric.type=\"%s\"", local.metric_data_availability_sync_duration),
        trimspace(local.filter_fragment_data_availability) == "" ? null : trimspace(local.filter_fragment_data_availability),
      ]))

      range {
        # 1 hour in seconds
        max = 3600
      }
    }
  }
}

# SLO 3: Requisition Fulfillment Timeliness
resource "google_monitoring_slo" "requisition_fulfillment_timeliness_99_9_28d" {
  service      = google_monitoring_custom_service.results_fulfiller.name
  slo_id       = "requisition-fulfillment-<1h-99-9-28d"
  display_name = "Requisition fulfillment latency <1h (28d)"
  goal                 = 0.999
  rolling_period_days  = 28

  request_based_sli {
    distribution_cut {
      distribution_filter = join(" ", compact([
        format("metric.type=\"%s\"", local.metric_requisition_fulfillment_latency),
        trimspace(local.filter_fragment_results_fulfiller) == "" ? null : trimspace(local.filter_fragment_results_fulfiller),
      ]))

      range {
        max = 3600
      }
    }
  }
}

# SLO 4: Fulfillment Correctness
resource "google_monitoring_slo" "fulfillment_correctness_99_99_28d" {
  service      = google_monitoring_custom_service.results_fulfiller.name
  slo_id       = "fulfillment-correctness-99-99-28d"
  display_name = "Fulfillment correctness (success / enqueued) (28d)"
  goal                 = 0.9999
  rolling_period_days  = 28

  request_based_sli {
    good_total_ratio {
      good_service_filter  = join(" ", compact([
        format("metric.type=\"%s\"", local.metric_requisitions_processed),
        # status label indicates success/failure; adjust to actual label key used in metric.
        "metric.labels.status=\"success\"",
        trimspace(local.filter_fragment_results_fulfiller) == "" ? null : trimspace(local.filter_fragment_results_fulfiller),
      ]))

      total_service_filter = join(" ", compact([
        format("metric.type=\"%s\"", local.metric_queue_writes),
        trimspace(local.filter_fragment_results_fulfiller) == "" ? null : trimspace(local.filter_fragment_results_fulfiller),
      ]))
    }
  }
}

# SLO 5: Event Group Sync Timeliness
resource "google_monitoring_slo" "event_group_sync_timeliness_99_9_28d" {
  service      = google_monitoring_custom_service.event_group_sync.name
  slo_id       = "event-group-sync-<20m-99-9-28d"
  display_name = "Event group sync latency <20m (28d)"
  goal                 = 0.999
  rolling_period_days  = 28

  request_based_sli {
    distribution_cut {
      distribution_filter = join(" ", compact([
        format("metric.type=\"%s\"", local.metric_event_group_sync_latency),
        trimspace(local.filter_fragment_event_group_sync) == "" ? null : trimspace(local.filter_fragment_event_group_sync),
      ]))

      range {
        # 20 minutes in seconds
        max = 1200
      }
    }
  }
}
