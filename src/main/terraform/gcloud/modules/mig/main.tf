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

  metadata_map = {
    "tee-image-reference"           = var.docker_image
    "tee-cmd"                       = jsonencode(var.tee_cmd)
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
  for_each = { for s in var.secrets_to_access : s.secret_id => s }

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
  family  = var.disk_image_family
  project = "confidential-space-images"
}

resource "terraform_data" "image_change_trigger" {
  input = var.docker_image
}

resource "google_compute_instance_template" "confidential_vm_template" {
  machine_type = var.machine_type

  confidential_instance_config {
    enable_confidential_compute = true
    confidential_instance_type  = "SEV"
  }

  scheduling {
    on_host_maintenance = "TERMINATE"
  }

  name_prefix = "${var.instance_template_name}-"
  lifecycle {
    replace_triggered_by  = [terraform_data.image_change_trigger]
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
    replace_triggered_by  = [terraform_data.image_change_trigger]
  }
  update_policy {
    type           = "PROACTIVE"
    minimal_action = "REPLACE"
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
