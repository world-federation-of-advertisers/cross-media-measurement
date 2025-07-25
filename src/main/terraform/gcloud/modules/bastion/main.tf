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

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.14.0"
    }
  }
}

# Bastion host instance
resource "google_compute_instance" "bastion" {
  name         = var.bastion_name
  machine_type = var.machine_type
  zone         = var.zone

  tags = ["bastion-host"]

  boot_disk {
    initialize_params {
      image = var.boot_image
      size  = var.boot_disk_size
    }
  }

  network_interface {
    network    = var.network_name
    subnetwork = var.subnetwork_name

    # External IP for SSH access from internet
    access_config {
      # Ephemeral external IP
    }
  }

  metadata = {
    enable-oslogin = "TRUE"
  }

  service_account {
    email  = var.service_account_email
    scopes = ["cloud-platform"]
  }
}

# Firewall rule to allow SSH to bastion from specific IPs
resource "google_compute_firewall" "bastion_ssh" {
  name    = "${var.bastion_name}-ssh"
  network = var.network_name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  target_tags   = ["bastion-host"]
  source_ranges = var.allowed_ssh_source_ranges
}

# Firewall rule to allow SSH from bastion to private instances
resource "google_compute_firewall" "ssh_from_bastion" {
  name    = "${var.bastion_name}-to-private"
  network = var.network_name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_tags = ["bastion-host"]
  target_tags = var.private_instance_tags
}