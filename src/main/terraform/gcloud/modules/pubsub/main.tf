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

data "google_project" "project" {}

resource "google_pubsub_topic" "topic" {
  project = data.google_project.project.name
  name    = var.topic_name
}

resource "google_pubsub_topic" "dead_letter_topic" {
  project = data.google_project.project.name
  name = "${var.topic_name}-dlq"
}

resource "google_pubsub_subscription" "subscription" {
  name  = var.subscription_name
  topic = google_pubsub_topic.topic.id

  ack_deadline_seconds       = var.ack_deadline_seconds
  message_retention_duration = var.subscription_queue_retention_period

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter_topic.id
    max_delivery_attempts = var.max_delivery_attempts
  }

  enable_exactly_once_delivery = true

}

resource "google_pubsub_topic_iam_member" "dead_letter_writer" {
  topic = google_pubsub_topic.dead_letter_topic.name
  role  = "roles/pubsub.publisher"
  member = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription" "dead_letter_subscription" {
  name  = "${var.topic_name}-dlq-sub"
  topic = google_pubsub_topic.dead_letter_topic.id

  ack_deadline_seconds = 30
}

resource "google_pubsub_topic_iam_member" "dead_letter_reader" {
  topic  = google_pubsub_topic.topic.name
  role   = "roles/pubsub.subscriber"
  member = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription_iam_member" "dead_letter_subscription_subscriber" {
  subscription = google_pubsub_subscription.subscription.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}
