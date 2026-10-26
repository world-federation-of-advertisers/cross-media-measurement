# Scheduled EventGroupSync over HTTP

This optional mode calls the existing EventGroupSync function directly, with no application
changes. Cloud Scheduler waits for the result for at most 30 minutes. The aggregator module
sets EGS to 29 minutes when at least one schedule is configured. Use this mode only when the
entire sync reliably finishes within that time, including expected growth and API throttling.

In the CMMS root configuration, set:

```hcl
event_group_sync_schedules = {
  egs-daily = {
    schedule = "0 2 * * *"
    time_zone = "Etc/UTC"
    data_provider = "dataProviders/example"
    input_blob_uri = "gs://example-bucket/provider/event_groups.json"
  }
}
```

The map key names the Scheduler job and its service account (use a valid service account ID).
An empty map keeps the existing deployment. `paused = true` allows provisioning without runs.

Before enabling a schedule, remove its event-group route from the DataWatcher config and
redeploy DataWatcher. Keep unrelated routes, including done-blob processing. Drain outstanding
event deliveries and confirm no sync is running before enabling the schedule. Do not enable
both scheduled and object-triggered syncs for the same provider. The request reads the current
object at the configured URI; it does not process every historical object generation.

Space schedules beyond the maximum run time, including schedules for other providers sharing
the function. A single-request instance limits concurrent processing, but is not a distributed
lock. Automatic Scheduler retries are disabled; a failed attempt is visible in Scheduler logs
and the next regular run retries the current object. Manual reruns and ambiguous HTTP failures
can repeat a sync. Monitor function failures and published mapping freshness.

Invocation IAM is scoped to the backing Cloud Run service. Cloud Scheduler API and its service
agent must be enabled as in the normal project setup. Terraform needs permission to create the
caller identity, attach it to Scheduler, and grant invocation on the function.

Run the infrastructure contract tests without cloud credentials or resource creation:

```sh
terraform init -backend=false
terraform test
```
