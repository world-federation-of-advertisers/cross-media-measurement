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

# SLO 1: API Availability (Public + Metadata APIs)
# - Goal: 99.9%, Window: 28d
# - SLI: Successful requests under 500ms (approximation using distribution cut on latency metric)

locals {
  # Metric for gRPC server latency exported by workloads running in Kubernetes
  # Note: This name reflects the planning document. Adjust if your metric type differs.
  api_latency_metric_type = "workload.googleapis.com/rpc.server.duration"

  # Optional filter fragments to scope to the specific services/components if needed
  # e.g., label filters like service.name or k8s container names can be added here.
  api_latency_additional_filter = ""
}

# Service definition that groups the Secure Computation APIs running on GKE.
resource "google_monitoring_custom_service" "secure_computation_apis" {
  service_id   = "secure-computation-apis"
  display_name = "Secure Computation APIs"
}

resource "google_monitoring_slo" "api_availability_99_9_28d" {
  service      = google_monitoring_custom_service.secure_computation_apis.name
  slo_id       = "api-availability-99-9-28d"
  display_name = "API availability: success <500ms (28d)"
  goal                 = 0.999
  rolling_period_days  = 28

  request_based_sli {
    # Approximation: fraction of requests (filtered to successful) with latency under 0.5s.
    # If you need to include all requests in the denominator (including errors),
    # switch to good_total_ratio and provide numerator/denominator filters accordingly.
    distribution_cut {
      distribution_filter = join(" ", compact([
        "resource.type=\"k8s_container\"",
        format("metric.type=\"%s\"", local.api_latency_metric_type),
        # Restrict to successful gRPC status codes only
        "metric.labels.rpc_grpc_status_code=\"0\"",
        trimspace(local.api_latency_additional_filter) == "" ? null : trimspace(local.api_latency_additional_filter),
      ]))

      range {
        # 500ms threshold
        max = 0.5
      }
    }
  }
}
