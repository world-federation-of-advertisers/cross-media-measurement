# Scheduled EventGroupSync caller job

This optional deployment uses Terraform only: Scheduler starts a Cloud Run Job through the
Jobs API and returns promptly. One stock-container task obtains an identity token, calls the
existing EGS HTTP function, and waits for its result. No application binary changes are needed.
The aggregator module allows EGS 60 minutes, the HTTP caller 65 minutes, and the task 70 minutes.

Configure the CMMS root:

```hcl
event_group_sync_jobs = {
  egs-daily = {
    schedule = "0 2 * * *"
    time_zone = "Etc/UTC"
    data_provider = "dataProviders/example"
    input_blob_uri = "gs://example-bucket/provider/event_groups.json"
    caller_image = "gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:<approved-image-digest>"
  }
}
```

Supply an approved stock image containing `/bin/sh` and `curl`, pinned by digest (for example
the Google Cloud CLI slim image). No custom image build is required. The map key names the job
and scheduler identity; `${name}-caller` must also fit the service account ID length limit.
An empty map preserves the current deployment. Set `paused = true` to provision without runs.

Before enabling, remove the corresponding event-group route from DataWatcher and redeploy it.
Preserve unrelated routes. Drain old event deliveries and wait for any active sync to finish.
Do not combine this trigger with DataWatcher or a direct Scheduler for the same provider.
Each execution reads the current object; it does not process every historical generation.

Cloud Scheduler success means the execution was accepted, not that EGS completed. Monitor
Cloud Run execution failures, EGS errors, and mapping freshness. A non-200 EGS response or
transport failure fails the task. The invocation uses the job identity's service-scoped
`roles/run.invoker`; Scheduler has invocation permission only on the job and uses OAuth.

Automatic Scheduler and task retries are disabled to avoid repeating an entire sync after an
ambiguous failure. The next regular schedule retries the current object. Space schedules beyond
70 minutes, including other providers on the shared function. `task_count = 1` and `parallelism = 1`
apply to each execution; Cloud Run Jobs can overlap separate executions. EGS retains a single
single-request instance, but this is not a distributed lock. Manual starts and ambiguous API
start responses can still repeat work.

This removes the source-event acknowledgement deadline from the invocation. It still relies
on a long HTTP connection to EGS: a disconnected caller can fail while EGS continues. Verify
long syncs in a test deployment before migration. Durable WorkItems provide completion tracking
and recovery if those guarantees are needed.

Cloud Run and Cloud Scheduler APIs and their service agents must be enabled. Terraform needs
permission to create/attach both identities and grant invocation on the function and job.

Run credential-free contract tests without creating cloud resources:

```sh
terraform init -backend=false
terraform test
python3 tests/caller_test.py
```
