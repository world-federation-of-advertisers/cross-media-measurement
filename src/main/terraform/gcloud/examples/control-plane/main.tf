# Copyright 2023 The Cross-Media Measurement Authors
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
  backend "gcs" {
    prefix = "terraform/state/halo-cmms"
  }
}

provider "google" { }

data "google_client_config" "default" {}

locals {
  pubsub_configurations = {
    "topic-demo"   = { subscription_name = "subscription-demo" }
  }
}

module "pubsub_topics" {
  for_each               = local.pubsub_configurations
  source                 = "../../modules/control-plane"
  topic_name             = each.key
  subscription_name      = each.value.subscription_name

  ack_deadline_seconds   = lookup(each.value, "ack_deadline_seconds", null)
  retain_acked_messages  = lookup(each.value, "retain_acked_messages", null)
  message_retention_duration = lookup(each.value, "message_retention_duration", null)
}

# module "pubsub_topics" {
#   for_each               = local.pubsub_configurations
#   source                 = "../../modules/control-plane"
#   topic_name             = each.key
#   subscription_name      = each.value.subscription_name
#   ack_deadline_seconds   = each.value.ack_deadline_seconds
#   retain_acked_messages  = each.value.retain_acked_messages
#   message_retention_duration = each.value.message_retention_duration
# }