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
its exact source URI/generation/creation time only in outer
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

## Automated online cutover (recommended)

Run **Update CMMS** with `apply=true` and `vid-dispatch-cutover-mode=online`
(the default), leaving `existing-image-tag` empty to publish the current compatible source. EDPs continue uploading raw files and publishing `done` markers.

The workflow prepares compatible dispatcher, DataWatcher, healing and control-plane
code while preserving the active watcher configuration. Terraform provisions the
queue, IAM and authenticated push delivery. The readiness command compares deployed
function source archives with the prepared compatible JARs, checks function/MIG/
control-plane readiness, and verifies push authentication and alert routes. One
isolated ordinary-dispatch/approved-recovery smoke test must complete through the
queue and emit successful application outcomes before activation.

Terraform then selects `data-watcher-config.dispatch-v2.textproto` and rolls both
cached consumers onto it. It retains `data-watcher-config.textproto` unchanged for
older revisions and rollback, so an idle old healing parser cannot read the typed
object. The workflow observes old watcher/healing invocations finishing and keeps
the compatible HTTP adapter. Subsequent updates preserve an already activated
configuration and do not repeat the migration smoke.

The existing CMMS control-plane compatibility barriers remain part of Update CMMS.
Intake stays available; downstream work can wait durably while those consumers roll.

### Operator profile and checks

Configure the environment secret `VID_DISPATCH_CUTOVER_PROFILE` for the isolated
test deployment:

| Field | Required content |
| --- | --- |
| `isolatedDataProvider` | Dedicated test DataProvider, raw/labeled prefixes and model lines. |
| `liveDataProviders` | Every other provider in the VID dispatcher configuration. |
| `ordinaryWorkItem` | Parameter-only WorkItem JSON for a fresh finalized raw marker, with outer `dataPathParams`. |
| `recoveryWorkItem` | Parameter-only WorkItem JSON carrying the approved source in `appParams.recovery.source` and its authorization references. |
| `approval` | Existing `ApproveUploadHealingOperationRequest` JSON, including the inspected etag and all candidate decisions for the isolated approval-required plan. |
| `tlsSecrets` | Secret Manager version resources keyed by the six existing EDPA/control-plane TLS flag names. |
| `edpaCertHost`, `controlPlaneCertHost` | Certificate hosts when they differ from the API target hosts. |

The TLS keys are `tls-cert-file`, `tls-key-file`, `cert-collection-file`,
`control-plane-tls-cert-file`, `control-plane-tls-key-file` and
`control-plane-cert-collection-file`. Values use
`projects/PROJECT/secrets/SECRET/versions/VERSION`. Certificates are read into private
temporary files and never printed.

Prepare the isolated raw baseline and correction plan with the existing upload/healing
tools. The smoke approves only that test plan and rejects providers listed as live.
Its ordinary source must not already have been admitted to the queue. Reused/stale
sources, changed approvals, failed executions and missing qualified application
outcomes fail the gate. After a partial smoke, retain and inspect its WorkItems;
another independent trial needs a fresh isolated baseline, not a rewritten production
marker.

The workflow invokes `.github/workflows/scripts/vid_dispatch_cutover.py`. Its
`preflight`, `ready` and `observe-legacy` commands use read-only APIs and fail closed
on missing access, incomplete inventories, stale metrics, incompatible code or unready
resources. Use the project, region and artifact arguments shown in
`terraform-cmms-v2.yml`. The checks never alter Terraform-owned resources.
The separate `smoke` command creates isolated application WorkItems and approves the
supplied test plan; it does not provision or modify infrastructure.

## Maintenance-window alternative

Use this shorter dispatch migration when a brief marker pause is acceptable:

1. Ask EDPs to pause new raw `done` markers. Raw files may continue uploading.
2. Run **Update CMMS** with `vid-dispatch-cutover-mode=maintenance` and the confirmed
   RFC 3339 UTC pause time in `vid-dispatch-markers-paused-at`. One checked preflight
   runs before deployment changes: Eventarc/watcher DLQ deliveries need two fresh
   zero-backlog samples, registrations must be complete, and no healing operation
   may be active or unresolved.
3. Let the workflow deploy the final stack with one final-stack Terraform apply,
   creating queue/IAM/push delivery and rolling compatible DataWatcher, dispatcher
   and healing together. The additional dispatch preparation apply is skipped.
   Existing CMMS control-plane compatibility barriers still run.
4. Wait for function/MIG/control-plane/queue readiness and the isolated queued
   dispatch/recovery smoke to pass.
5. Resume raw `done` markers.

A timeout or empty log search does not prove HTTP work finished. Missing or stale
evidence fails the check. Correct the reported condition and rerun the command;
do not acknowledge or delete pending notifications to bypass it.

## Manual sequencing fallback

Use the dual-route sequence only when the workflow cannot prove readiness:

1. Deploy compatible dispatcher/control-plane/DataWatcher/healing code while
   retaining the HTTP watcher configuration.
2. Provision queue, registry, authenticated push delivery and DLQ listener through
   the normal deployment/Terraform workflow, then pass the same readiness checks.
3. Pass the isolated queued-dispatch/recovery smoke, then activate the committed
   queued configuration in Dev, followed by Head and QA.
4. Observe legacy HTTP deliveries finishing and retain the adapter for rollback.

Do not use targeted component deployments to bypass a failed readiness gate or
an incomplete CMMS compatibility rollout.

## Rollback

Direct Terraform applies default `activate_vid_labeling_dispatch_config` to false;
the gated workflow supplies true for activation.

Apply Terraform with `activate_vid_labeling_dispatch_config=false` to restore the
retained legacy config. Keep queue resources, registry mapping, queued jobs and
compatible worker code. Never downgrade the worker while queued work remains.

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

Inspect a failed dispatch WorkItem before retrying. Use the existing `VidLabelingHeal`
tool and the same control-plane connection/TLS flags as `retry-failed`. A Picocli
argument file can hold those flags once:

```shell
bazel run //src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:VidLabelingHeal -- \
  get-dispatch-work-item workItems/WORK_ITEM_ID @control-plane.flags

bazel run //src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:VidLabelingHeal -- \
  retry-dispatch-work-item workItems/WORK_ITEM_ID \
  --expected-work-item-generation=INSPECTED_GENERATION @control-plane.flags
```

Inspection prints execution generation, state and stored ordinary/recovery source,
including malformed parameters that may explain a failure. Retry requires `FAILED`
and the exact positive execution generation inspected. It refuses `RUNNING`,
unsupported states and generation mismatches before issuing RetryWorkItem. The server
checks that generation transactionally again against concurrent retries. The command
prints the returned generation/state and exits nonzero on RPC failure; it never
automatically resubmits an uncertain retry.

Low-level fallback:

```shell
grpcurl -cert CLIENT_CERT_PEM -key CLIENT_KEY_PEM -cacert TRUSTED_ROOTS_PEM \
  -authority CONTROL_PLANE_CERT_HOST \
  -d '{"name":"workItems/WORK_ITEM_ID"}' CONTROL_PLANE_TARGET \
  wfa.measurement.securecomputation.controlplane.v1alpha.WorkItems/GetWorkItem

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
