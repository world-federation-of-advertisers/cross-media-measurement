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

data "google_project" "project" {}

locals {

  tee_cmd = [
    "--edpa-tls-cert-secret-id", "edpa-tee-app-tls-pem",
    "--edpa-tls-key-secret-id", "edpa-tee-app-tls-key",
    "--secure-computation-cert-collection-secret-id", "securecomputation-root-ca",
    "--kingdom-cert-collection-secret-id", "kingdom-root-ca",
    "--edp-name", "edp7",
    "--edp-cert-der-secret-id", "edp7-cert-der",
    "--edp-private-der-secret-id", "edp7-private-der",
    "--edp-enc-private-secret-id", "edp7-enc-private",
    "--edp-tls-key-secret-id", "edp7-tls-key",
    "--edp-tls-pem-secret-id", "edp7-tls-pem",
    "--kingdom-public-api-target", "v2alpha.kingdom.dev.halo-cmm.org:8443",
    "--secure-computation-public-api-target", "v1alpha.secure-computation.dev.halo-cmm.org:8443",
    "--subscription-id", "results-fulfiller-subscription",
    "--google-project-id", "halo-cmm-dev",
    "--event-template-metadata-blob-uri", "gs://edpa-configs-storage-dev-bucket/results_fulfiller_event_proto_descriptor.pb"
  ]

  metadata_map = {
    "tee-image-reference"           = var.docker_image
    "tee-container-log-redirect"    = "true"
    "tee-cmd"                       = jsonencode(local.tee_cmd)
  }
}

resource "google_service_account" "mig_service_account" {
  account_id   = var.mig_service_account_name
  description  = "Service account for Managed Instance Group"
  display_name = "MIG Service Account"
}

resource "google_service_account_iam_member" "allow_terraform_to_use_mig_sa" {
  service_account_id = google_service_account.mig_service_account.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}

resource "google_pubsub_subscription_iam_member" "mig_subscriber" {
  subscription  = var.subscription_id
  role          = "roles/pubsub.subscriber"
  member        = "serviceAccount:${google_service_account.mig_service_account.email}"
}

resource "google_secret_manager_secret_iam_member" "mig_sa_secret_accessor" {
  for_each = { for s in var.secrets_to_mount : s.secret_id => s }

  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.mig_service_account.email}"
}

resource "google_project_iam_member" "mig_sa_user" {
  project = data.google_project.project.name
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.mig_service_account.email}"
}

resource "google_project_iam_member" "confidential_workload_user" {
  project  = data.google_project.project.name
  role     = "roles/confidentialcomputing.workloadUser"
  member   = "serviceAccount:${google_service_account.mig_service_account.email}"
}

resource "google_project_iam_member" "mig_log_writer" {
  project = data.google_project.project.name
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.mig_service_account.email}"
}

data "google_compute_image" "confidential_space" {
  family  = "confidential-space-debug"
  project = "confidential-space-images"
}

resource "google_compute_instance_template" "confidential_vm_template" {
  machine_type = var.machine_type

  confidential_instance_config {
    enable_confidential_compute = true
    confidential_instance_type  = "SEV_SNP"
  }

  scheduling {
    on_host_maintenance = "TERMINATE"
  }

  name_prefix = "${var.instance_template_name}-"
  lifecycle {
    create_before_destroy = true
  }

  disk {
    boot            = true
    source_image    = data.google_compute_image.confidential_space.self_link
  }

  shielded_instance_config {
    enable_secure_boot = true
  }

  network_interface {
    network = "default"
    access_config { }
  }

  metadata = merge(
      {
        "google-logging-enabled"    = "true"
        "google-monitoring-enabled" = "true"
      },
      local.metadata_map

    )
  service_account {
    email = google_service_account.mig_service_account.email
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/pubsub"
    ]
  }
}

resource "google_compute_region_instance_group_manager" "mig" {
  name               = var.managed_instance_group_name
  base_instance_name = var.base_instance_name
  version {
    instance_template = google_compute_instance_template.confidential_vm_template.id
  }
  distribution_policy_zones = var.mig_distribution_policy_zones
  lifecycle {
    create_before_destroy = true
  }
}

resource "google_compute_region_autoscaler" "mig_autoscaler" {
  name   = "autoscaler-for-${google_compute_region_instance_group_manager.mig.name}"
  target = google_compute_region_instance_group_manager.mig.id

  autoscaling_policy {
    max_replicas = var.max_replicas
    min_replicas = var.min_replicas

    metric {
      name                       = "pubsub.googleapis.com/subscription/num_undelivered_messages"
      filter                     = "resource.type = pubsub_subscription AND resource.labels.subscription_id = \"${var.subscription_id}\""
      single_instance_assignment = var.single_instance_assignment
    }
  }
}
