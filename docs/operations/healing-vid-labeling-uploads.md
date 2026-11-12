# Heal VID-labeling uploads

The VID-labeling dispatcher automatically quarantines a raw-upload revision when it edits or
removes a previously registered object. The healing controller combines pending candidates into
one DataProvider-scoped plan, pauses ordinary processing, and waits for one operator decision.

The dispatcher and healing controller use `RAW_IMPRESSION_UPLOAD_TARGET` and
`RAW_IMPRESSION_UPLOAD_CERT_HOST` for the public EDP Aggregator API. Their existing
mTLS certificate and CA mappings also authenticate correction and healing calls.
Controller environment values, secret mappings, and labeled-output deletion prefixes
are configured in each environment's Terraform variables file.

The update workflow prepares IAM and configuration while consumers are quiesced,
rolls out the API server, and then deploys the function revisions. The healing
schedule remains paused during that preparation.


## Deployment configuration

The scheduled healing controller is disabled until its private persistence endpoint
is provisioned. In the environment's committed Terraform tfvars, set
`vid_labeling_healing_controller_enabled = true` together with
`vid_labeling_healing_controller_env_var`,
`vid_labeling_healing_controller_secret_mapping`, and
`vid_labeling_healing_labeled_output_object_prefixes`. The runtime settings must
include the public raw-upload and control-plane targets, the private healing target,
their TLS authorities, and the matching TLS Secret Manager mounts. Restrict deletion
prefixes to the configured VidLabeler output directories, each ending in `/`.

Terraform requires the healing target, TLS mounts, and deletion prefixes when
enabled. When disabled, it creates no healing function, schedule, access grants, or
alerts. The dispatcher still quarantines corrections; automatic healing progression
requires the configured controller.


CMMS configurations keep externally produced impressions under `edp/...` and
VidLabeler output under `vid-labeled-impressions/edp/...`. The registrar's
`DataAvailabilitySyncConfig.vid_labeled_impression_path` must match the deployed
VidLabeling output path. External uploads keep `edp_impression_path` and their
DataWatcher notifications.

Deploy the updated availability functions and configuration before resuming VID
labeling. A queued WorkItem retains its encoded output path; the registrar accepts
both configured paths. Existing external data does not need to be moved or
regenerated. Healing earlier internal output may require its earlier labeling
configuration and prefix.

## Upgrade legacy healing operations

Before the obsolete-field migration, keep older healing controllers and clients quiesced so they
cannot create or advance legacy plans during the schema update. The migration halts before changing
the table if a version-one operation, including a row with `NULL` plan format, is not `COMPLETE`.
Inspect the affected operations before upgrading:

```sql
SELECT DataProviderResourceId, UploadHealingOperationId, State, PlanFormatVersion
FROM UploadHealingOperation
WHERE (PlanFormatVersion IS NULL OR PlanFormatVersion = 1)
  AND State <> 2;
```

`2` is the stored `COMPLETE` state. Complete any returned legacy repairs using a compatible release
before retrying the upgrade. Do not mark an interrupted repair complete or release its fence merely
to bypass the guard. Completed legacy rows and active candidate-backed version-two plans can
migrate; their remaining operation and step data stay readable, and each old `CreateRequestId`
becomes its `ReconcileRequestId`.

## Inspect a correction

Alerts identify the affected DataProvider and plan. List the plans requiring a decision or manual
attention:

```shell
vid-labeling-heal list-correction-plans \
  --data-provider=dataProviders/DATA_PROVIDER \
  --state=APPROVAL_REQUIRED,NEEDS_ATTENTION \
  --edpa-public-api-target=EDPA_TARGET \
  TLS_FLAGS
```

Inspect the plan before approving it:

```shell
vid-labeling-heal get-correction-plan \
  dataProviders/DATA_PROVIDER/uploadHealingOperations/OPERATION \
  --edpa-public-api-target=EDPA_TARGET \
  TLS_FLAGS
```

The plan contains every correction candidate, historical owner, memoized cascade step, and
non-memoized local step. Its `etag` changes whenever a pending candidate or computed step changes.
The controller supplies the labeled-output prefix when it creates or reconciles a draft, and the
operation persists that prefix. Availability handoff checks it against current controller
configuration; restore the matching configuration if that check fails on resume. Candidate-linked
steps identify the uploads whose derived state is invalid. Retention remains controller
configuration; an operation does not persist a retention cutoff. Retention is enforced while the
draft remains mutable; approval freezes the step graph, so later controller progression does not
reject it merely because the configured window advanced.

List candidate summaries when the plan needs more investigation:

```shell
vid-labeling-heal list-correction-candidates \
  --data-provider=dataProviders/DATA_PROVIDER \
  --state=ASSIGNED \
  --edpa-public-api-target=EDPA_TARGET \
  TLS_FLAGS

vid-labeling-heal get-correction-candidate \
  dataProviders/DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/CANDIDATE \
  --edpa-public-api-target=EDPA_TARGET \
  TLS_FLAGS
```

The list output omits raw object URIs. The authenticated get command returns the detailed manifest
comparison and historical owners.

## Approve once

Assign exactly one decision to every candidate:

- `CORRECT` evicts affected derived state and replays the candidate's exact stored done-object
  generation.
- `NO_REPLACEMENT` evicts the removed upload and rebuilds later memoized dependencies without it.

```shell
vid-labeling-heal approve-correction-plan \
  dataProviders/DATA_PROVIDER/uploadHealingOperations/OPERATION \
  --correct-candidate=dataProviders/DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/C1 \
  --no-replacement-candidate=dataProviders/DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/C2 \
  --etag=PLAN_ETAG \
  --edpa-public-api-target=EDPA_TARGET \
  TLS_FLAGS
```

The draft is a versioned provisional plan. It fixes every affected upload/model-line row and
labeled-output URI, including in-flight rows in a memoized cascade, without claiming that the
currently visible object generations are final. Approval atomically freezes the candidate set and
decisions, transfers the candidate fence to the operation, defers queued uploads, and enters
`DRAINING`. Operators do not pause `DataAvailabilitySync` or run eviction, replay, or resume
commands.

## New revisions during an approved repair

A repair becomes committed when its operation enters `EVICTING`. Later raw-upload revisions
remain quarantined or deferred and cannot cancel that running plan. Recovery processes the
retained approved object generations and verifies the replacements recorded by that operation.
After the repair completes, newer corrections are planned against the current output owners and
require a new operator approval. If a retained object version is unavailable, the operation needs
attention; current directory contents are not substituted for the approved data.

Existing labeling work, incomplete registrations, and synchronization leases finish before the
fence enters `EVICTING`. Only then does the controller capture and persist the exact output
generations before deleting anything. An interrupted controller safely resumes either the drain or
exact-version finalization.

Eviction deletes obsolete versioned output; any stale availability WorkItem later fails its
exact-generation check. A replacement uses the normal VID-labeling path: VidLabeler writes the
versioned output `done` object, moves the model line to `AVAILABILITY_SYNCING` with its pending
event dates, and defers availability publication while the eviction fence is active. When
replacement labeling is durable, the operation enters `AVAILABILITY_HANDOFF_PENDING` and
atomically releases the fence. The controller then creates one exact-generation Data Availability
WorkItem per pending model-line date and marks the operation `COMPLETE` after every WorkItem is
durably scheduled or already exists with the same identity. It does not wait for those WorkItems to
synchronize. Each acknowledged event date is removed from the pending set; the model line becomes
`COMPLETED` only after that set is empty.

Uploads received during healing remain deferred by the healing dispatch gate even during the brief
post-fence handoff. A later correction does not acquire another fence while a preceding replacement
remains `AVAILABILITY_SYNCING`, preventing its queued WorkItems from exhausting retries behind the
new fence.

## Monitor automatic progression

The plan moves through these states:

| State | Meaning |
| --- | --- |
| `APPROVAL_REQUIRED` | The complete mutable draft is ready for an operator decision. |
| `APPROVED` | Decisions are immutable and the controller may claim the plan. |
| `DRAINING` | Existing dispatch and availability work is finishing. |
| `EVICTING` | Derived state and labeled output are being invalidated. |
| `REPLAYING` | Exact approved EDP correction generations are being replayed. |
| `RECOVERING` | Dependent memoized uploads are rebuilding in chronological order. |
| `AVAILABILITY_HANDOFF_PENDING` | The fence is released and exact availability WorkItems are being durably scheduled. |
| `NEEDS_ATTENTION` | A non-retryable failure requires inspection and an explicit retry. |
| `COMPLETE` | Every step completed and every deferred availability handoff was scheduled. |

Transient failures are retried automatically. Alerts cover pending approvals, stalled controllers,
manifest mismatches, out-of-retention candidates, and plans in `NEEDS_ATTENTION`.

## Retry a plan needing attention

Resolve the reported cause, retrieve the current plan and etag, then retry it once:

```shell
vid-labeling-heal retry-correction-plan \
  dataProviders/DATA_PROVIDER/uploadHealingOperations/OPERATION \
  --etag=PLAN_ETAG \
  --edpa-public-api-target=EDPA_TARGET \
  TLS_FLAGS
```

The command is rejected unless the plan is in `NEEDS_ATTENTION`. The controller resumes from its
durable checkpoints; the CLI never advances individual workflow steps.

## Validate automatic healing

Use a dedicated test DataProvider, raw and labeled-output prefixes, EventGroups, and model lines.
A different folder under a production DataProvider does not isolate a memoized recovery cascade.
Record the deployed revision and configuration before starting. Prepare small uploads for three
successive event dates, D1 through D3, with one memoized and one non-memoized model line. Follow the
[reference VID upload guide](../edpaggregator/reference-vid-impression-upload-guide.md). Wait for
each upload to complete labeling and availability synchronization before finalizing the next date.

Run each scenario on a fresh baseline. The controlled drain case below uses its own in-process
fixture, and the retention case requires an older registration rather than a newly created baseline.
Record baseline WorkItems, object generations, metadata, and snapshots so existing work is not
mistaken for work created by a correction.

1. **Inspect, approve, and replay a correction.** Replace one D2 raw object, retain the other
   objects, and write a new `done` generation after the directory is final. Inspect the candidate
   and its `APPROVAL_REQUIRED` plan with the commands above. Verify the stored prior and current
   object generations, the D2 steps for both model lines, and the dependent D3 memoized step.
   Confirm the candidate creates no labeling work before approval. Approve it with `CORRECT` and
   the current plan etag. Leave the raw directory unchanged while the controller evicts and
   replays the approved generation, then recovers D3. Verify that corrected D2 records appear
   once, D3 uses the rebuilt memoized predecessor, and the unaffected direct D3 output is unchanged.
2. **Reject a changed approved manifest before commitment.** On a separate D1–D3 baseline,
   publish a D2 correction and wait for its `APPROVAL_REQUIRED` plan. In a dedicated test deployment,
   pause the healing controller's Scheduler job using the command below. Wait for any current
   controller invocation to finish, then confirm the plan still requires approval. Approve it with
   `CORRECT` while the scheduler is paused. Before the operation enters `EVICTING`, overwrite one D2
   raw object again without rewriting `done`, then resume the scheduler. The next manifest check
   must leave the operation in `NEEDS_ATTENTION`, with no eviction or replay checkpoint, no labeling
   WorkItem for the approved correction, and unchanged baseline output, metadata, and snapshots.
   Repeat on independent baselines with a raw object added or removed instead of overwritten.
   Finalize the changed directory with a newer `done` generation. Verify that the old candidate
   becomes `SUPERSEDED`, the newer candidate is included in a refreshed `APPROVAL_REQUIRED` plan,
   and its new etag requires renewed approval; the operation resource may be reused. No labeling
   work may start for the newer candidate before that approval. Approve the refreshed plan and
   verify successful recovery before ending the case. If the operation already entered `EVICTING`
   before the raw mutation, repeat on a fresh baseline: committed repairs must finish their approved
   retained generations as described in [New revisions during an approved repair](#new-revisions-during-an-approved-repair).
3. **Quarantine and heal a mixed revision.** On a fresh baseline, register D2 objects A and B,
   then append C in a second D2 revision. Complete both revisions before finalizing D3. Create one
   complete mixed revision by overwriting A with corrected same-date data
   A′, removing B, retaining C unchanged, and adding D. Write `done` last. Verify a `MIXED` candidate
   containing the complete A′/C/D manifest, the original owners of A and B, and their added, edited,
   and removed differences. Before approval, the entire generation must be quarantined: neither D
   nor any other part of the correction may create a labeling WorkItem, and baseline output must
   remain unchanged. Approve with `CORRECT` and wait for healing and availability completion. For
   each D2 model line, verify exactly one labeled data object and matching sidecar for A′, C, and D,
   with `ACTIVE` metadata pointing to the replacement upload and current output generations. The
   old A contents and B must contribute no records; B's data and sidecar must be absent and its
   existing metadata `DELETED`. Verify D3's rebuilt memoized lineage and unchanged direct output.
   Repeat addition+edit and addition+removal on separate baselines. Each final raw-to-labeled
   manifest and metadata set must match its complete approved revision, with no partial visibility.
4. **Remove an upload without replacement.** Remove every D2 raw object and write a new `done`
   generation. Inspect the removal candidate and approve it with `NO_REPLACEMENT`. Verify that
   D2 has no replacement output or active metadata, while memoized D3 rebuilds from retained D1.
   The direct D3 output must remain unchanged.
5. **Drain existing work and defer new uploads.** Run the controlled drain test linked below on
   its isolated baseline. It withholds VidLabeler publication to keep a later memoized model line
   in `LABELING`, acquires an active Data Availability lease, and then publishes and approves a
   correction to its predecessor. Verify `DRAINING`, provisional output manifests, unchanged output
   generations and snapshots, and rejection of a new lease. Publish another ordinary upload while
   the fence is held and verify that its labeling remains deferred. Release the held labeling work
   with `republishQueuedVidLabelers()` and release the lease with `releaseDataAvailabilitySyncLease()`.
   Observe eviction, replay, recovery, and availability handoff; the deferred upload must then
   complete without rewriting its `done` marker. These are test-harness controls, not deployed
   worker pause commands. Do not manually pause `DataAvailabilitySync`.
6. **Retry from a durable checkpoint.** On test output only, temporarily hold an exact GCS object
   generation that the plan must delete. Approve the correction and inspect the resulting
   `NEEDS_ATTENTION` operation, its saved resume state, and its completed steps. Release the hold,
   retrieve the current etag, and invoke `retry-correction-plan`. Verify that
   automatic progression resumes from the saved state, preserves completed checkpoints, and
   finishes with one replacement per affected upload/model line. No operator eviction, replay,
   or resume command is part of this procedure.
7. **Reject a correction outside the healing retention window.** Use a separate test DataProvider
   with a completed D2 owner registered before the controller's retention cutoff and retained raw
   and labeled objects. An old impression event date alone does not make its registration old.
   Record its current output, metadata, snapshots, and WorkItems, then edit one raw object and
   finalize a correction. Run the controller and verify the candidate enters
   `MANUAL_INTERVENTION_REQUIRED` and the operation is inspectable in `NEEDS_ATTENTION`, with a
   reason identifying the retention failure. Observe the corresponding healing metric and
   Monitoring incident; verify notification delivery only where channels are configured. No
   replay, correction labeling WorkItem, output deletion, metadata deletion, or snapshot
   deletion may occur. Run the controller again and redeliver the same finalized generation:
   the candidate must remain non-dispatchable, baseline data must remain unchanged, and the
   revision must not fall back to ordinary registration or wait for routine operator approval.
   The automated retention case linked below creates this boundary with a test-only short window;
   it does not backdate production records or change the market's retention configuration.

### Controller scheduling control

Use the Scheduler job configured by `vid_labeling_healing_controller_scheduler_config`. Replace
`HEALING_CONTROLLER_JOB`, `LOCATION`, and `PROJECT` with its deployed identifiers. The job invokes
the controller for all configured DataProviders, so use these commands only in a dedicated test
deployment; use the in-process regression for a shared environment.

```shell
gcloud scheduler jobs pause HEALING_CONTROLLER_JOB --location=LOCATION --project=PROJECT
```

Pausing does not cancel an invocation already running. Wait for its completion in the function
logs before approving the negative-case plan. Resume after the raw mutation, and restore the
original scheduling state when the case finishes or fails:

```shell
gcloud scheduler jobs resume HEALING_CONTROLLER_JOB --location=LOCATION --project=PROJECT
```

### Automated validation references

The [pre-commit manifest regression](../../src/test/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/spanner/VidLabelingHealingChronologyIntegrationTest.kt)
drives controller invocations explicitly on an isolated baseline, verifies that a changed approved
manifest causes no eviction or replay, and verifies supersession and renewed approval after a newer
finalized revision:

```shell
bazel test //src/test/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/spanner:VidLabelingHealingChronologyIntegrationTest \
  --test_filter='manifest mismatch before eviction reopens the plan after a newer correction'
```

The drain and retention harness cases are supplied by [PR #4588](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/4588).
Run their commands from a checkout containing that PR, such as the complete stack.

* [Controlled drain case](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/e6d1f4f895e85f127dd7f7a6455c1b9720e48fc1/src/test/kotlin/org/wfanet/measurement/edpaggregator/service/v1alpha/VidLabelingHealingHandoffIntegrationTest.kt):
  `withholdNextVidLabelerPublications()` holds publication, not a running worker thread. The test
  acquires the lease with `acquireDataAvailabilitySyncLease()` and releases it using its name and
  etag with `releaseDataAvailabilitySyncLease()`. `republishQueuedVidLabelers()` releases labeling.
  The publication controls belong to `VidLabelingPipelineTestHarness`; lease RPCs use the real
  in-process lease service. Each test creates its own storage and services.
* [Mixed quarantine and retention cases](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/e6d1f4f895e85f127dd7f7a6455c1b9720e48fc1/src/test/kotlin/org/wfanet/measurement/edpaggregator/service/v1alpha/VidLabelingRevisionHealingIntegrationTest.kt):
  the mixed case checks quarantine for each combination. The retention case checks
  `MANUAL_INTERVENTION_REQUIRED`, `NEEDS_ATTENTION`, and unchanged metadata; the no-work and
  monitoring checks in scenario 7 remain part of deployed validation.

```shell
bazel test //src/test/kotlin/org/wfanet/measurement/edpaggregator/service/v1alpha:VidLabelingHealingHandoffIntegrationTest \
  --test_filter='draining freezes output while active memoized labeling and availability lease finish'
bazel test //src/test/kotlin/org/wfanet/measurement/edpaggregator/service/v1alpha:VidLabelingRevisionHealingIntegrationTest \
  --test_filter='out of retention correction enters operator attention without eviction'
```

Historical deployed results are recorded in the
[V2 validation plan](https://docs.google.com/document/d/1hBsL2z1VWSbW5yMj1mUDS4XsCp2qD-SRgkcuaiu0WE0/edit):
Test 79 covers the pre-commit raw mutation, and Tests 34 and 37 cover full mixed revisions.
Record a new execution against the deployed revision; earlier results do not certify the current
stack, and an incomplete or failed journey is not a passed validation.

At a blocked negative-case boundary, require unchanged baseline data and no correction work. In
the retention case, `MANUAL_INTERVENTION_REQUIRED` and `NEEDS_ATTENTION` are the expected result;
the correction remains quarantined and no replacement completion or fence release is expected.

After successful healing, inspect the operation and replacement model lines separately. An operation
can be `COMPLETE` after availability WorkItems are durably scheduled while replacement model lines
remain `AVAILABILITY_SYNCING`. Require successful availability WorkItems, empty pending event-date
sets, and `COMPLETED` replacement model lines before accepting the final data result. Verify the
exact live data and sidecar generations, active or deleted `ImpressionMetadata`, memoized snapshot
lineage, and the absence of the completed operation's eviction fence. Execute a measurement that
covers the affected dates and verify that corrected contributions appear once and removed
contributions are absent.

Record each scenario's actual result, observed operation states, candidate and replacement names,
WorkItem outcomes, exact object generations, and expected versus actual measurement results.
A missing test control or unavailable measurement environment makes that assertion unexecuted;
record it explicitly rather than reporting the whole scenario as passed. These steps describe
validation to perform and do not certify a deployment or a previously completed test run.
