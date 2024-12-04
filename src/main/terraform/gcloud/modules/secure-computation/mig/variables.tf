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

variable "single_instance_assignment" {
  description   = "The amount of undelivered messages a single instance can handle. Used by the autoscaler to determine the number of instances needed based on the total number of undelivered messages."
  type          = number
  nullable      = false
}

variable "subscription_id" {
  description   = "The subscription used to determine the amount of undelivered messages."
  type          = number
  nullable      = false
}

variable "managed_instance_group_name" {
  description   = "The name of the Managed Instance Group."
  type          = string
  nullable      = false
}

variable "max_replicas" {
  description   = "The maximum number of instances that can be deployed in this Managed Instance Group (MIG). This defines the upper limit for autoscaling."
  type          = number
  nullable      = false
}

variable "min_replicas" {
  description   = "The minimum number of instances that must always be running in this Managed Instance Group (MIG), even during low-load periods."
  type          = number
  nullable      = false
}

variable machine_type {
  description   = "The machine type to create."
  type          = string
  nullable      = false
}