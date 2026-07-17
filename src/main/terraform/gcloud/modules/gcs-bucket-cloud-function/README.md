# GCS-Triggered Cloud Function

Terraform module to deploy a Cloud Function (Gen2) triggered by GCS object
events (finalize or delete), with a Pub/Sub dead letter queue for
undeliverable notifications.

## Dead Letter Queue

Eventarc creates a Pub/Sub push subscription to deliver GCS notifications to
the Cloud Function. If the Cloud Function is unavailable (e.g. during
redeployment or when Cloud Run has scaled to zero and cold-start fails), the
push subscription retries with exponential backoff. Without a dead letter
queue, messages that cannot be delivered within the retention period are
silently dropped.

This module creates a DLQ topic and subscription for each function, and
attaches it to the Eventarc-managed subscription after deployment. Failed
messages are preserved for 7 days in the DLQ subscription for inspection
and replay.

## Cloud Run Service Configuration

After deploying the Cloud Function, operators should configure the underlying
Cloud Run service for production workloads. These settings are not managed by
Terraform because `gcloud functions deploy` resets them on each deployment;
apply them after deployment or via a CI post-deploy step.

```bash
gcloud run services update <FUNCTION_NAME> \
  --region=<REGION> \
  --min-instances=0 \
  --max-instances=10 \
  --concurrency=1 \
  --timeout=120s \
  --cpu=0.5 \
  --memory=512Mi
```

### Recommended values

| Setting | Value | Rationale |
|---|---|---|
| `min-instances` | `0` | Default. Cold starts (~5s) are well within the 600s ack deadline. The DLQ preserves messages during extended outages (redeployment, infrastructure failures). Set to `1` if low-latency event processing is required. |
| `max-instances` | `10` | Limits concurrent instances during burst processing (e.g. GCS lifecycle deleting hundreds of files). Adjust based on downstream capacity (Spanner, Kingdom API). |
| `concurrency` | `1` | Each invocation processes a single GCS event. Serial processing avoids contention on shared resources (gRPC channels, Spanner transactions). |
| `timeout` | `120s` | Most invocations complete in under 1 second. The 120s ceiling accommodates cold starts (~5s) and occasional slow downstream RPCs. |
| `cpu` | `0.5` | GCS-triggered functions (data-watcher, data-watcher-delete) are lightweight event routers that match a config and forward an HTTP POST. 0.5 vCPU is sufficient. Downstream functions that do heavier work (Spanner writes, KMS decryption) should use 1 vCPU. |
| `memory` | `512Mi` | Matches the `--memory=512MB` in the deploy command. Increase if the function loads large configurations or handles high-cardinality tracing. |
