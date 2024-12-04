# Copyright 2024 The Cross-Media Measurement Authors
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

resource "google_compute_instance_template" "confidential_vm_template" {
  machine_type   = var.machine_type

  confidential_instance_config {
      enable_confidential_compute = true
      confidential_instance_type  = "SEV_SNP"
    }

  disks {
      source_image  = "projects/cos-cloud/global/images/family/cos-stable"
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
  restartPolicy: Always
EOT
  }

  service_account {
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}

resource "google_compute_region_instance_group_manager" "mig" {
  name               = var.managed_instance_group_name
  base_instance_name = "tee-app"
  version {
      instance_template = google_compute_instance_template.confidential_vm_template.id
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
