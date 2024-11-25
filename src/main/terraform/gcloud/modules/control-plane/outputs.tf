output "pubsub_topic" {
  value       = google_pubsub_topic.topic.name
  description = "Name of the created Pub/Sub topic"
}

output "pubsub_subscription" {
  value       = google_pubsub_subscription.subscription.name
  description = "Name of the created Pub/Sub subscription"
}