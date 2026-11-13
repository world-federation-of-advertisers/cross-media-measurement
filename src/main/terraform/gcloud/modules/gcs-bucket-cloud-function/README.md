# GCS-Triggered Cloud Function

Terraform module to deploy a Cloud Function (Gen2) triggered by GCS object
events (finalize or delete, with optional additional events), with a Pub/Sub dead
letter queue for undeliverable notifications.

## Additional storage events

Set `additional_trigger_event_types = ["archived"]` to send archive notifications
from a versioned bucket to the same function. Deleting a live object without a
generation archives it; overwriting an object also archives its previous version.
A cleanup handler must check whether a live version remains before deleting its
metadata. Additional triggers use the existing trigger identity and dead-letter
queue.

## Dead Letter Queue

Eventarc creates a Pub/Sub push subscription to deliver GCS notifications to
the Cloud Function. Google Cloud does not retry failed invocations by default.
This module defaults `retry_on_failure` to `true` and explicitly passes
`--retry`, enabling redelivery with exponential backoff. Without a dead letter
queue, messages that cannot be delivered within the retention period are silently
dropped.

This module creates a DLQ topic and subscription for each function, and
attaches it to the Eventarc-managed subscription after deployment. Failed
messages are preserved for 7 days in the DLQ subscription for inspection
and replay.

## Cloud Run Service Configuration

After deploying the Cloud Function, operators should configure the underlying
Cloud Run service for production workloads. Most settings below are not managed
by Terraform because `gcloud functions deploy` resets them on each deployment;
apply them after deployment or via a CI post-deploy step. Function timeout is
the exception and can be set with this module's `timeout_seconds` input.

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
| `timeout` | `120s` | Most invocations complete in under 1 second. Set `timeout_seconds` when the handler synchronously waits for downstream work; the VID-labeling DataWatcher uses the event-driven maximum of 540s. |
| `cpu` | `0.5` | GCS-triggered functions (data-watcher, data-watcher-delete) are lightweight event routers that match a config and forward an HTTP POST. 0.5 vCPU is sufficient. Downstream functions that do heavier work (Spanner writes, KMS decryption) should use 1 vCPU. |
| `memory` | `512Mi` | Matches the `--memory=512MB` in the deploy command. Increase if the function loads large configurations or handles high-cardinality tracing. |

The VID-labeling DataWatcher waits for a dispatcher whose timeout is 480 seconds, leaving one minute
of caller headroom.

## Tests

The native Terraform tests live under `src/test/terraform`. Terraform requires a
local test directory, so run them from this module with a temporary copy:

```bash
archive_test_dir=$(mktemp -d tests.XXXXXX)
trap 'rm -rf "$archive_test_dir"' EXIT
cp ../../../../../test/terraform/gcloud/modules/gcs-bucket-cloud-function/*.tftest.hcl "$archive_test_dir/"
terraform init -backend=false
terraform test -test-directory="$archive_test_dir"
```

These tests use a mock Google provider and only plan resources.
