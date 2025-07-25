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

resource "google_kms_crypto_key_iam_member" "mig_kms_user" {
  crypto_key_id = var.kms_key_id
  role          = "roles/cloudkms.cryptoKeyDecrypter"
  member        = "serviceAccount:${google_service_account.mig_service_account.email}"
}

resource "google_secret_manager_secret_iam_member" "mig_sa_secret_accessor" {
  for_each = { for s in var.secrets_to_mount : s.secret_id => s }

  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.mig_service_account.email}"
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
    source_image = "projects/cos-cloud/global/images/family/cos-stable"
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
      {
        # 1) at boot, loop over each secret and write it out:
        "startup-script" = <<-EOT
          #!/bin/bash
          set -euo pipefail
          %{ for s in var.secrets_to_mount }
          host_dir="$(dirname "${s.mount_path}")"
          mkdir -p "$host_dir"
          docker run --rm \
            -v "$host_dir":"$host_dir" \
            gcr.io/google.com/cloudsdktool/cloud-sdk:slim \
            gcloud secrets versions access ${s.version} \
              --secret=${s.secret_id} \
              --project=${data.google_project.project.name} \
              --out-file=${s.mount_path}
          chmod 644 ${s.mount_path}
          %{ endfor }
          EOT
      },
      {
        # 2) append a --<flag>=<path> for each mount into the container args
        "gce-container-declaration" = <<-EOT
  spec:
    containers:
      - name: subscriber
        image: ${var.docker_image}
        stdin: false
        tty: false
        args: ${jsonencode(
          concat(
            var.app_args,
            [ for s in var.secrets_to_mount :
              "${s.flag_name}=${s.mount_path}"
              if s.flag_name != null
            ]
          )
        )}
        env:
          - name: GRPC_TRACE
            value: "all"
          - name: GRPC_VERBOSITY
            value: "DEBUG"
        volumeMounts:
          - name: ssl-secrets
            mountPath: /etc/ssl
            readOnly: true
          - name: proto-descriptors
            mountPath: /var/tmp
            readOnly: true
    restartPolicy: Always
    volumes:
      - name: ssl-secrets
        hostPath:
          path: /etc/ssl
          type: DirectoryOrCreate
      - name: proto-descriptors
        hostPath:
          path: /var/tmp
          type: DirectoryOrCreate
  EOT
      }
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
