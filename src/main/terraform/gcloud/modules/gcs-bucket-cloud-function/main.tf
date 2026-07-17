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
data "google_client_config" "default" {}

resource "google_service_account" "cloud_function_service_account" {
  account_id   = var.cloud_function_service_account_name
  display_name = "Service account for Cloud Function"
}

resource "google_service_account" "cloud_function_trigger_service_account" {
  account_id   = var.cloud_function_trigger_service_account_name
  display_name = "Trigger Service Account for Cloud Function"
}

resource "google_service_account_iam_member" "allow_terraform_to_use_cloud_function_service_account" {
  service_account_id = google_service_account.cloud_function_service_account.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}

resource "google_service_account_iam_member" "allow_terraform_to_use_data_watcher_trigger_service_account" {
  service_account_id = google_service_account.cloud_function_trigger_service_account.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}

resource "google_storage_bucket_iam_member" "cloud_function_object_viewer" {
  bucket = var.trigger_bucket_name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.cloud_function_service_account.email}"
}

resource "google_storage_bucket_iam_member" "cloud_function_object_creator" {
  bucket = var.trigger_bucket_name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.cloud_function_service_account.email}"
}

resource "google_project_iam_member" "trigger_event_receiver" {
  project = data.google_project.project.id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.cloud_function_trigger_service_account.email}"
}

resource "google_project_iam_member" "trigger_run_invoker" {
  project = data.google_project.project.id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.cloud_function_trigger_service_account.email}"
}

resource "google_secret_manager_secret_iam_member" "secret_accessor" {
  for_each  = toset(var.secrets_to_access)
  secret_id = each.value
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.cloud_function_service_account.email}"
}

# Dead letter topic for undeliverable GCS event notifications.
resource "google_pubsub_topic" "dead_letter_topic" {
  name = "${var.function_name}-dlq"
}

resource "google_pubsub_subscription" "dead_letter_subscription" {
  name  = "${var.function_name}-dlq-sub"
  topic = google_pubsub_topic.dead_letter_topic.id

  message_retention_duration = "604800s" # 7 days
  ack_deadline_seconds       = 30
}

# Allow the Pub/Sub service agent to publish to the DLQ topic.
resource "google_pubsub_topic_iam_member" "dead_letter_publisher" {
  topic  = google_pubsub_topic.dead_letter_topic.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Allow the Pub/Sub service agent to ack messages on the Eventarc-managed
# subscription (required for dead-letter forwarding).
resource "google_project_iam_member" "pubsub_subscriber" {
  project = data.google_project.project.id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "terraform_data" "deploy_gcs_cloud_function" {

  depends_on = [
    google_service_account.cloud_function_service_account,
    google_service_account.cloud_function_trigger_service_account,
    google_service_account_iam_member.allow_terraform_to_use_cloud_function_service_account,
    google_service_account_iam_member.allow_terraform_to_use_data_watcher_trigger_service_account,
    google_storage_bucket_iam_member.cloud_function_object_viewer,
    google_storage_bucket_iam_member.cloud_function_object_creator,
    google_project_iam_member.trigger_event_receiver,
    google_project_iam_member.trigger_run_invoker,
    google_secret_manager_secret_iam_member.secret_accessor,
  ]

  triggers_replace = [
    var.uber_jar_path,
    var.extra_env_vars,
    var.secret_mappings,
    var.uploaded_config_generation,
  ]

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    environment = {
      FUNCTION_NAME           = var.function_name
      ENTRY_POINT             = var.entry_point
      CLOUD_REGION            = data.google_client_config.default.region
      RUN_SERVICE_ACCOUNT     = google_service_account.cloud_function_service_account.email
      TRIGGER_BUCKET          = var.trigger_bucket_name
      TRIGGER_SERVICE_ACCOUNT = google_service_account.cloud_function_trigger_service_account.email
      EXTRA_ENV_VARS          = var.extra_env_vars
      SECRET_MAPPINGS         = var.secret_mappings
      UBER_JAR_DIRECTORY      = dirname(var.uber_jar_path)
      TRIGGER_EVENT_TYPE      = var.trigger_event_type
    }
    command = <<-EOT
      #!/bin/bash
      set -euo pipefail

      args=(
        "functions" "deploy" "$FUNCTION_NAME"
        "--gen2"
        "--runtime=java17"
        "--entry-point=$ENTRY_POINT"
        "--memory=512MB"
        "--region=$CLOUD_REGION"
        "--run-service-account=$RUN_SERVICE_ACCOUNT"
        "--source=$UBER_JAR_DIRECTORY"
        "--trigger-event-filters=type=google.cloud.storage.object.v1.$TRIGGER_EVENT_TYPE"
        "--trigger-event-filters=bucket=$TRIGGER_BUCKET"
        "--trigger-service-account=$TRIGGER_SERVICE_ACCOUNT"
        "--no-allow-unauthenticated"
      )

      if [[ -n "$EXTRA_ENV_VARS" ]]; then
        args+=("--set-env-vars=$EXTRA_ENV_VARS")
      fi

      if [[ -n "$SECRET_MAPPINGS" ]]; then
        args+=("--set-secrets=$SECRET_MAPPINGS")
      fi

      gcloud "$${args[@]}"
    EOT
  }
}

# Attach the DLQ to the Eventarc-managed Pub/Sub subscription after
# deployment. Eventarc creates the subscription implicitly; this step
# patches it with a dead-letter policy so undeliverable messages are
# preserved instead of silently dropped.
resource "terraform_data" "attach_dead_letter_policy" {
  depends_on = [
    terraform_data.deploy_gcs_cloud_function,
    google_pubsub_topic.dead_letter_topic,
    google_pubsub_topic_iam_member.dead_letter_publisher,
    google_project_iam_member.pubsub_subscriber,
  ]

  triggers_replace = [
    var.uber_jar_path,
    var.extra_env_vars,
    var.secret_mappings,
    var.uploaded_config_generation,
  ]

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    environment = {
      FUNCTION_NAME    = var.function_name
      CLOUD_REGION     = data.google_client_config.default.region
      DLQ_TOPIC        = google_pubsub_topic.dead_letter_topic.id
      MAX_DELIVERY_ATTEMPTS = tostring(var.max_delivery_attempts)
      MESSAGE_RETENTION     = var.message_retention_duration
    }
    command = <<-EOT
      #!/bin/bash
      set -euo pipefail

      # Find the Eventarc-managed subscription for this function.
      SUB=$(gcloud pubsub subscriptions list \
        --filter="name~eventarc.*-$FUNCTION_NAME-" \
        --format="value(name)" \
        --limit=1)

      if [[ -z "$SUB" ]]; then
        echo "WARNING: No Eventarc subscription found for $FUNCTION_NAME. Skipping DLQ attachment."
        exit 0
      fi

      echo "Attaching DLQ to subscription: $SUB"
      gcloud pubsub subscriptions update "$SUB" \
        --dead-letter-topic="$DLQ_TOPIC" \
        --max-delivery-attempts="$MAX_DELIVERY_ATTEMPTS" \
        --message-retention-duration="$MESSAGE_RETENTION"
    EOT
  }
}

# Alert when messages land in the dead letter queue.
resource "google_monitoring_alert_policy" "dlq_alert" {
  display_name = "${var.function_name} DLQ messages"
  combiner     = "OR"

  conditions {
    display_name = "Undelivered DLQ messages"

    condition_threshold {
      filter          = "resource.type = \"pubsub_subscription\" AND resource.labels.subscription_id = \"${google_pubsub_subscription.dead_letter_subscription.name}\" AND metric.type = \"pubsub.googleapis.com/subscription/num_undelivered_messages\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "60s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }

  notification_channels = var.alert_notification_channels

  documentation {
    content = "Messages are landing in the dead letter queue for ${var.function_name}. This means GCS event notifications failed delivery to the Cloud Function after ${var.max_delivery_attempts} attempts. Inspect the DLQ subscription (${var.function_name}-dlq-sub) and replay the messages."
  }
}
