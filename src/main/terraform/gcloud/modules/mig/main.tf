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
#   role          = "roles/pubsub.subscriber"
  role          = "roles/pubsub.admin"
  member        = "serviceAccount:${google_service_account.mig_service_account.email}"
}

resource "google_kms_crypto_key_iam_member" "mig_kms_user" {
  crypto_key_id = var.kms_key_id
  role          = "roles/cloudkms.cryptoKeyDecrypter"
  member        = "serviceAccount:${google_service_account.mig_service_account.email}"
}

resource "google_secret_manager_secret_iam_member" "mig_sa_secret_accessor" {
  for_each = { for s in var.secrets_to_mount : s.secret_key => s }

  secret_id = var.secrets[each.key].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.mig_service_account.email}"
}

# resource "google_compute_firewall" "allow-egress-all" {
#   name    = "allow-egress-all"
#   network = "default"
#
#   direction = "EGRESS"
#
#   allow {
#     protocol = "tcp"
#     ports    = ["443", "80"]
#   }
#
#   destination_ranges = ["0.0.0.0/0"]
#   priority           = 1000
#
#   target_service_accounts = [google_service_account.mig_service_account.email]
# }

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
#   disk {
#     source_image = "ubuntu-os-cloud/ubuntu-2004-lts"
#   }

  network_interface {
    network = "default"
    access_config { }
  }

#   metadata = merge(
#     {
#       "google-logging-enabled"    = "true"
#       "google-monitoring-enabled" = "true"
#       "startup-script" = <<-EOT
#         #!/bin/bash
#         set -euo pipefail
#
#         echo "Installing Docker..."
#         apt-get update
#         apt-get install -y docker.io
#         systemctl start docker
#         systemctl enable docker
#
#         echo "Retrieving secrets..."
#         %{ for s in var.secrets_to_mount }
#         TOKEN=$(curl -s -H "Metadata-Flavor: Google" \
#           http://metadata/computeMetadata/v1/instance/service-accounts/default/token \
#           | jq -r .access_token)
#
#         curl -s -H "Authorization: Bearer $TOKEN" \
#           "https://secretmanager.googleapis.com/v1/projects/${data.google_project.project.name}/secrets/${var.secrets[s.secret_key].secret_id}/versions/${s.version}:access" \
#           | jq -r .payload.data \
#           | base64 --decode > ${s.mount_path}
#         chmod 644 ${s.mount_path}
#         %{ endfor }
#
#         echo "Running container..."
#         docker run -d --name=subscriber --restart=always \\
#           --network=host \\
#           -v /etc/ssl:/etc/ssl:ro \\
#           ${join(" ", concat(
#             [for s in var.secrets_to_mount : "--env ${s.flag_name}=${s.mount_path}" if s.flag_name != null],
#             [var.docker_image],
#             var.app_args
#           ))}
#       EOT
#     }
#   )

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
          # get an access token from the metadata server
          TOKEN=$(curl -s -H "Metadata-Flavor: Google" \
            http://metadata/computeMetadata/v1/instance/service-accounts/default/token \
            | jq -r .access_token)

          curl -s -H "Authorization: Bearer $TOKEN" \
            "https://secretmanager.googleapis.com/v1/projects/${data.google_project.project.name}/secrets/${var.secrets[s.secret_key].secret_id}/versions/${s.version}:access" \
            | jq -r .payload.data \
            | base64 --decode > ${s.mount_path}
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
    restartPolicy: Always
    volumes:
      - name: ssl-secrets
        hostPath:
          path: /etc/ssl
          type: DirectoryOrCreate
  EOT
      }
    )
#     - name: GRPC_DEFAULT_SSL_ROOTS_FILE_PATH
#                 value: "/etc/ssl/ca-certificates.crt"
#           - name: JAVA_TOOL_OPTIONS
#             value: "-Djavax.net.ssl.trustStore=/etc/ssl/ca-certificates.crt \
#                     -Djavax.net.ssl.trustStorePassword=changeit"
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
  distribution_policy_zones = [
    "us-central1-a"
  ]
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
