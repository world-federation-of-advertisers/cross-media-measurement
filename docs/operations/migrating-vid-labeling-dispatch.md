# Migrate VID dispatch to WorkItems

Raw-upload notifications use `vid-labeling-dispatch-queue`. The existing dispatcher
Cloud Function accepts Pub/Sub push WorkItems and legacy DataWatcher HTTP requests.
The worker uses the common WorkItemRunner for leased attempts and durable outcomes.
Its 480-second function timeout, one-instance limit, one-request concurrency, and
existing RPC pacing remain unchanged. The queue uses a 600-second acknowledgment
deadline, up to 100 approximate delivery attempts, and 10–600-second backoff.

## Payload and recovery

`WorkItem.WorkItemParams.app_params` contains `VidLabelingDispatchWorkItemParams`.
It carries the provider and optional recovery references. Ordinary dispatch carries
its exact source URI/generation/creation time/event type only in outer
`data_path_params`, enabling standard source admission and indexed lookup.
Recovery instead carries that complete source in `app_params.recovery.source`,
with the source upload and authorizing UploadHealingOperation resource names.
The dispatcher derives authorized ModelLines from that operation's selected
recovery-target steps, then validates checkpoint, source and row authorization.
Recovery leaves outer source unset so approved historical replay is not suppressed.

The stable intake ID includes source identity and dispatch intent. Ordinary intake
and approved correction replay of one generation are distinct jobs. Equivalent
recovery deliveries deduplicate. Downstream upload and child-job request IDs remain
unchanged. A successful dispatch job means registration/dispatch invocation finished;
labeling and availability continue separately. Successful quarantine and legitimate
no-work outcomes complete intake. The existing best-effort fast-path/Monitor backstop
remains unchanged.

For a custom manifest using `#SecureComputation`, enable
`_vidLabelingDispatchQueueEnabled` only when the dispatch queue is provisioned.
The generic definition defaults to disabled; the CMMS manifest enables it alongside
its committed Terraform queue configuration.

## Deploy in order

1. Deploy the compatible dispatcher worker before updating DataWatcher intake.
   Then deploy the lease-aware control plane, DataWatcher, and healing controller
   while retaining the existing watcher configuration.
2. Provision the queue, authenticated push delivery, queue registry entry, and generic
   DLQ listener. Terraform orders watcher config publication after worker/queue readiness.
3. Validate an isolated test provider's queued dispatch and recovery. Then adopt the
   committed queued dispatcher routes in Dev, followed by Head and QA. Upgrade the
   healing controller's embedded watcher/parser before adopting typed config.
4. Let old HTTP deliveries finish. Keep the compatible legacy adapter while resolving
   in-flight work and preserving rollback. Other watcher routes retain their contracts.

If all changes are in one release, perform a preparation deployment with the earlier
watcher configuration before the activation deployment. A source merge alone does not
prove deployed worker/parser readiness. Preserve queued jobs, topics, registry mapping,
and worker code during rollback; restore the previous watcher configuration to return
new notifications to the earlier route. Never downgrade the worker while queued jobs
remain.

## Failure signals and cutover readiness

Dispatch alert policies count terminal worker failures and confirmed exhausted
deliveries from payload-free lifecycle logs. The exhaustion event precedes the
DLQ acknowledgment, so draining the subscription does not erase the signal.
Inspect the event's WorkItem name, execution generation, attempt/error evidence
when present, and stored application source. An upload record may not exist.

TODO(world-federation-of-advertisers/cross-media-measurement#4912): Configure
approved operator notification channels in Dev, Head and QA, verify receipt of
a test notification, and record dispatch alert/route readiness before cutover.
The policies can create incidents with empty channels during preparation, but
queued dispatch must not be activated until this readiness is verified.

## Inspect and retry

Inspect a failed WorkItem, its attempts, and its source before retrying: outer
`data_path_params` for ordinary work or `app_params.recovery.source` for replay.
Use the existing WorkItems API with the exact execution generation inspected:

```shell
grpcurl -cert CLIENT_CERT_PEM -key CLIENT_KEY_PEM -cacert TRUSTED_ROOTS_PEM \
  -authority CONTROL_PLANE_CERT_HOST \
  -d '{"name":"workItems/WORK_ITEM_ID","expectedWorkItemGeneration":"GENERATION"}' \
  CONTROL_PLANE_TARGET \
  wfa.measurement.securecomputation.controlplane.v1alpha.WorkItems/RetryWorkItem
```

Execution generation belongs to the WorkItem; it differs from the GCS object
version in the selected source's `generation`. Retry advances execution generation while
retaining source identity and original parameters. Duplicate source notifications do
not restart terminal jobs. Correct invalid configuration/authorization before retrying;
use a new `done` generation only for an actual revised upload, not to restart a job.

Pub/Sub handles retry timing and exhausted-delivery DLQ forwarding. The Secure
Computation generic listener records terminal WorkItem failure. Attempt history is
for diagnosis; the deprecated queue `max_work_item_attempts` field is not the retry
counter. WorkItems carry metadata references only; keep payload-free trace attributes.
