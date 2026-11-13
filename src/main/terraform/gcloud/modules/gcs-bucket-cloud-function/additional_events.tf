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

# A versioned bucket archives a live object when it is deleted without a generation.
# Additional triggers let the same handler receive archive and permanent-delete events.
resource "google_eventarc_trigger" "additional_storage_event" {
  for_each = setsubtract(var.additional_trigger_event_types, toset([var.trigger_event_type]))

  depends_on = [terraform_data.deploy_gcs_cloud_function]

  name            = "${var.function_name}-${each.value}"
  location        = data.google_client_config.default.region
  service_account = google_service_account.cloud_function_trigger_service_account.email

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.${each.value}"
  }
  matching_criteria {
    attribute = "bucket"
    value     = var.trigger_bucket_name
  }
  destination {
    cloud_run_service {
      service = var.function_name
      region  = data.google_client_config.default.region
    }
  }
}

resource "terraform_data" "attach_additional_dead_letter_policy" {
  for_each = google_eventarc_trigger.additional_storage_event

  depends_on = [
    google_pubsub_topic_iam_member.dead_letter_publisher,
    google_project_iam_member.pubsub_subscriber,
  ]

  triggers_replace = [
    each.value.uid,
    google_pubsub_topic.dead_letter_topic.id,
    var.max_delivery_attempts,
    var.message_retention_duration,
  ]

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    environment = {
      SUBSCRIPTION          = each.value.transport[0].pubsub[0].subscription
      DLQ_TOPIC             = google_pubsub_topic.dead_letter_topic.id
      MAX_DELIVERY_ATTEMPTS = tostring(var.max_delivery_attempts)
      MESSAGE_RETENTION     = var.message_retention_duration
    }
    command = <<-EOT
      set -euo pipefail
      gcloud pubsub subscriptions update "$SUBSCRIPTION" \
        --dead-letter-topic="$DLQ_TOPIC" \
        --max-delivery-attempts="$MAX_DELIVERY_ATTEMPTS" \
        --message-retention-duration="$MESSAGE_RETENTION"
    EOT
  }
}
