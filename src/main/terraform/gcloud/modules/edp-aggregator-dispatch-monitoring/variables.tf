# Copyright 2026 The Cross-Media Measurement Authors
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

variable "project_id" {
  description = "Project containing the dispatcher, generic listener logs and dispatch queue."
  type        = string
}

variable "dead_letter_subscription_id" {
  description = "Short Pub/Sub subscription ID consumed by the generic DLQ listener."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z][A-Za-z0-9_.~+%-]+$", var.dead_letter_subscription_id))
    error_message = "Use a short Pub/Sub subscription ID, without a projects/subscriptions prefix."
  }
}

variable "notification_channels" {
  description = "Approved Monitoring notification channels; configuration and receipt verification remain tracked by #4912."
  type        = list(string)
  default     = []
}
