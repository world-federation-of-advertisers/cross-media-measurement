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

# OpenTelemetry Collector on Cloud Run
# Central collector for Cloud Functions and Confidential MIG telemetry

# Service account for the OTel Collector
resource "google_service_account" "otel_collector" {
  account_id   = "edpa-otel-collector"
  display_name = "EDPA OpenTelemetry Collector"
  description  = "Service account for EDPA OpenTelemetry Collector on Cloud Run"
}

# Allow Terraform service account to deploy using the OTel Collector service account
resource "google_service_account_iam_member" "allow_terraform_to_use_otel_collector_sa" {
  service_account_id = google_service_account.otel_collector.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}

# Grant Monitoring Metric Writer role to the collector
resource "google_project_iam_member" "otel_collector_metric_writer" {
  project = data.google_project.project.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.otel_collector.email}"
}

# Grant Cloud Trace Agent role for trace export (future use)
resource "google_project_iam_member" "otel_collector_trace_agent" {
  project = data.google_project.project.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${google_service_account.otel_collector.email}"
}

# Upload the OTel Collector configuration to GCS
resource "google_storage_bucket_object" "otel_collector_config" {
  name   = "otel-collector-config.yaml"
  bucket = module.config_files_bucket.storage_bucket.name
  source = "${path.module}/otel-collector-config.yaml"
}

# Cloud Run service for OTel Collector
resource "google_cloud_run_v2_service" "otel_collector" {
  name     = "edpa-otel-collector"
  location = data.google_client_config.default.region
  # Internal traffic only (VPC and same-project Cloud Run services)
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.otel_collector.email

    # Keep at least 1 instance warm to avoid cold starts
    scaling {
      min_instance_count = 1
      max_instance_count = 10
    }

    containers {
      image = "otel/opentelemetry-collector-contrib:0.129.1"

      # Pass config via environment variable (base64 encoded by Cloud Run)
      args = ["--config=env:OTEL_CONFIG"]

      env {
        name  = "OTEL_CONFIG"
        value = file("${path.module}/otel-collector-config.yaml")
      }

      resources {
        limits = {
          cpu    = "2"
          memory = "1Gi"
        }
      }

      ports {
        # Using h2c (HTTP/2 Cleartext) for gRPC per Cloud Run requirements
        # Rationale:
        # - Cloud Run requires h2c port name for gRPC services
        # - Cloud Run terminates TLS and routes HTTPS:443 -> HTTP/2:8080
        # - Reference: https://cloud.google.com/run/docs/triggering/grpc
        name           = "h2c"
        container_port = 8080
      }

      # Health check endpoint
      startup_probe {
        http_get {
          path = "/"
          port = 13133
        }
        initial_delay_seconds = 5
        timeout_seconds       = 3
        period_seconds        = 10
        failure_threshold     = 3
      }

      liveness_probe {
        http_get {
          path = "/"
          port = 13133
        }
        initial_delay_seconds = 10
        timeout_seconds       = 3
        period_seconds        = 30
      }
    }

  }

  depends_on = [
    google_service_account_iam_member.allow_terraform_to_use_otel_collector_sa,
    google_project_iam_member.otel_collector_metric_writer,
    google_project_iam_member.otel_collector_trace_agent,
  ]
}

# Allow unauthenticated invocations from internal Google Cloud traffic only
# Security: Combined with INGRESS_TRAFFIC_INTERNAL_AND_CLOUD_LOAD_BALANCING,
# only services within the same Google Cloud project can reach this endpoint.
# This simplifies OTLP client configuration (no auth tokens needed).
resource "google_cloud_run_v2_service_iam_member" "allow_internal_unauthenticated" {
  name     = google_cloud_run_v2_service.otel_collector.name
  location = google_cloud_run_v2_service.otel_collector.location
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# Output the collector URL for configuration
output "otel_collector_grpc_endpoint" {
  description = "OTLP gRPC endpoint for telemetry export. Clients should use: -Dotel.exporter.otlp.endpoint=<this-url> -Dotel.exporter.otlp.protocol=grpc"
  value       = google_cloud_run_v2_service.otel_collector.uri
}
