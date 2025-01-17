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

resource "google_service_account" "mig_service_account" {
  account_id   = var.mig_service_account_name
  description  = "Service account for Managed Instance Group"
  display_name = "MIG Service Account"
}

resource "google_pubsub_topic_iam_member" "mig_pubsub_user" {
  topic  = var.topic_id
  role   = "roles/pubsub.subscriber"
  member = "serviceAccount:${google_service_account.mig_service_account.email}"
}

resource "google_storage_bucket_iam_member" "mig_storage_viewer" {
  bucket = var.storage_bucket_name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.mig_service_account.email}"
}

resource "google_storage_bucket_iam_member" "mig_storage_creator" {
  bucket = var.storage_bucket_name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.mig_service_account.email}"
}

resource "google_kms_crypto_key_iam_member" "mig_kms_user" {
  crypto_key_id = var.kms_key_id
  role          = "roles/cloudkms.cryptoKeyDecrypter"
  member        = "serviceAccount:${google_service_account.mig_service_account.email}"
}

resource "google_compute_instance_template" "confidential_vm_template" {
  machine_type   = var.machine_type

  confidential_instance_config {
      enable_confidential_compute = true
      confidential_instance_type  = "SEV_SNP"
  }

  name = var.instance_template_name

  disks {
      source_image  = "projects/cos-cloud/global/images/family/cos-stable"
  }

  network_interface {
      network = "default"  # TODO(@marcopremier): Add VPC here.
    }

  metadata = {
    "google-logging-enabled" = "true"
    "google-monitoring-enabled" = "true"
    "gce-container-declaration" = <<EOT
spec:
  containers:
    - name: subscriber
      image: ${var.docker_image}
      stdin: false
      tty: false
      args: ${jsonencode(var.app_args)}
  restartPolicy: Always
EOT
  }

  service_account {
      email = google_service_account.mig_service_account.email
  }
}

resource "google_compute_region_instance_group_manager" "mig" {
  name               = var.managed_instance_group_name
  base_instance_name = var.base_instance_name
  version {
    instance_template = google_compute_instance_template.confidential_vm_template.id
  }

  update_policy {
    type                    = "PROACTIVE"
    minimal_action          = "RESTART"
    max_unavailable_fixed   = 1
    replacement_method      = "RECREATE"
  }

}

resource "google_compute_autoscaler" "mig_autoscaler" {
  name   = "autoscaler-for-${google_compute_instance_group_manager.mig.name}"
  target = google_compute_instance_group_manager.mig.id

  autoscaling_policy {
    max_replicas    = var.max_replicas
    min_replicas    = var.min_replicas

    metric {
      name                       = "pubsub.googleapis.com/subscription/num_undelivered_messages"
      filter                     = "resource.type = pubsub_subscription AND resource.label.subscription_id = \"${var.subscription_id}\""
      single_instance_assignment = var.single_instance_assignment
    }
  }
}
