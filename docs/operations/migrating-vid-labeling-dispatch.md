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
queue, IAM and authenticated push delivery. Readiness compares deployed source
archives with the prepared compatible JARs and verifies functions, MIGs, control
plane, push authentication and alert routes.

A normal Terraform apply then selects a versioned canary configuration: the committed
isolated EDP7 test provider uses the queue, while live providers retain HTTP delivery.
Both cached consumers roll onto that configuration only after compatible code is
verified. The workflow finalizes a fresh ordinary test `done` object, discovers its
deterministic DataWatcher WorkItem, and waits for the worker outcome. It approves the supplied isolated operator-recovery plan and waits for the healing
controller to record the fresh marker generation. RecoverUploader owns that rewrite;
the smoke reads the recorded version and discovers its DataWatcher WorkItem.
Both markers must have a queue-write log from the actual DataWatcher Cloud Function,
as well as successful worker outcomes. The controller's direct replay cannot substitute
for storage-event intake. No smoke WorkItem is created directly.

Only after both actual storage-event journeys and application outcomes pass does the
final apply switch live providers to queued delivery. Terraform retains
`data-watcher-config.textproto` for old revisions and rollback; the versioned key
isolates an idle old healing parser from typed configuration.

The workflow captures legacy revision identities and declared request lifetimes
before each cutover apply. After zero traffic is confirmed, it waits the maximum
captured lifetime while checking that traffic stays zero, then rechecks traffic and
fresh zero-backlog evidence from the source subscription. Idle revisions may scale
to zero; fresh container-concurrency samples are not required. Missing lifetime,
ambiguous traffic or pending source deliveries fails the gate. The HTTP adapter stays
available for rollback. Subsequent updates preserve a fully activated configuration
and do not repeat the migration smoke.

The existing CMMS control-plane compatibility barriers remain part of Update CMMS.
Intake stays available; downstream work can wait durably while those consumers roll.

### Operator profile and checks

Stable configuration is committed in
`.github/env-configs/{dev,head,qa}/vid-dispatch-cutover.json`. It contains the isolated
test provider, other configured providers, allowed raw prefix, watcher identifier
and Secret Manager version references. Provider/model-line routing comes from the
committed dispatcher configuration. Certificate/key contents remain in Secret Manager;
version aliases are resolved to numbered versions before access, and private temporary
files are never printed.

For the initial cutover, supply `vid-dispatch-smoke-inputs` as JSON for this workflow
run only:

| Field | Required content |
| --- | --- |
| `ordinarySourceUri` | A new isolated raw `done` path that does not yet exist; upload its raw fixture files first. |
| `recoverySourceUri` | The current isolated historical marker that the controller will rewrite for an operator-recovery target. |
| `recoverySourceGeneration` | The inspected historical generation belonging to that source upload; a changed current marker fails closed. |
| `recoverySourceUpload` | The isolated RawImpressionUpload authorizing that recovery. |
| `approval` | The freshly inspected ApproveUploadHealingOperationRequest for a candidate-backed plan containing the selected operator-recovery target, including name, etag and candidate decisions. |

Both URIs must remain under the committed isolated raw prefix and must be different.
The workflow creates the ordinary marker with a does-not-exist precondition, reads
its new generation/creation time, and binds discovery to that identity. Recovery uses the controller-recorded generation and verifies the complete source-upload/operation metadata.
Do not reuse prior WorkItem JSON, generations or approval etags as permanent configuration.

Prepare the isolated raw baseline and approval-required plan containing a dependent operator-recovery target using the
existing upload/healing tools. Select a current source whose operator recovery creates
a new marker generation; candidate-correction replay and deferred-source reuse do not
exercise a fresh recovery storage event and are rejected by this smoke. The smoke rejects other providers. Reused sources,
changed approvals, failed executions, missing intake and missing qualified application
outcomes fail the gate. After a partial run, retain and inspect the printed source URIs
and deterministic WorkItem names; another trial needs a fresh isolated ordinary source
and a currently valid recovery plan. Never rewrite a production marker for this check.

The workflow invokes `.github/workflows/scripts/vid_dispatch_cutover.py`. Its
`preflight`, `ready` and `observe-legacy` commands use read-only APIs and fail closed
on missing access, incomplete inventories, stale metrics, incompatible code or unready
resources. Use the project, region and artifact arguments shown in
`terraform-cmms-v2.yml`. The checks never alter Terraform-owned resources.
The separate `smoke` command finalizes isolated storage objects and approves the
supplied test plan. DataWatcher creates the application WorkItems; the operator CLI
discovers them by deterministic identity. These application mutations do not change
Terraform-owned infrastructure.

## Maintenance-window alternative

Use this shorter dispatch migration when a brief marker pause is acceptable:

1. Ask EDPs to pause new raw `done` markers. Raw files may continue uploading.
2. Run **Update CMMS** with `vid-dispatch-cutover-mode=maintenance` and the confirmed
   RFC 3339 UTC pause time in `vid-dispatch-markers-paused-at`, and the fresh one-run
   `vid-dispatch-smoke-inputs`. One checked preflight
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
