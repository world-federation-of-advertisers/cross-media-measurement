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

variable "bastion_name" {
  description = "Name of the bastion host instance"
  type        = string
  default     = "bastion-host"
}

variable "machine_type" {
  description = "Machine type for the bastion host"
  type        = string
  default     = "e2-micro"
}

variable "zone" {
  description = "Zone where the bastion host will be created"
  type        = string
}

variable "network_name" {
  description = "Name of the VPC network"
  type        = string
}

variable "subnetwork_name" {
  description = "Name of the subnetwork"
  type        = string
}

variable "boot_image" {
  description = "Boot image for the bastion host"
  type        = string
  default     = "debian-cloud/debian-11"
}

variable "boot_disk_size" {
  description = "Boot disk size in GB"
  type        = number
  default     = 10
}

variable "service_account_email" {
  description = "Service account email for the bastion host"
  type        = string
}

variable "allowed_ssh_source_ranges" {
  description = "List of IP ranges allowed to SSH to the bastion host"
  type        = list(string)
  default     = ["0.0.0.0/0"] # Restrict this in production!
}

variable "private_instance_tags" {
  description = "Network tags of private instances that bastion can SSH to"
  type        = list(string)
  default     = []
}