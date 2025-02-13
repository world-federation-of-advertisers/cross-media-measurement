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

variable "topic_name" {
  description = "Name of the Pub/Sub topic"
  type        = string
  nullable    = false
}

variable "subscription_name" {
  description = "Name of the Pub/Sub subscription"
  type        = string
  nullable    = false
}

variable "subscription_queue_retention_period" {
  description = "The duration (in seconds) for which the subscription queue retains unacknowledged messages."
  type        = string
  default     = null
}

variable "ack_deadline_seconds" {
  description = "The time (in seconds) allowed for subscribers to acknowledge messages. If the acknowledgment period is not extended or the message is not acknowledged within this time, the message will be re-delivered."
  type        = number
  default     = null
}

variable "max_delivery_attempts" {
  description = "The maximum number of delivery attempts for a message before it is routed to the dead letter queue."
  type        = number
  default     = null
}
