# Heal VID-labeling uploads

The VID-labeling dispatcher automatically quarantines a raw-upload revision when it edits or
removes a previously registered object. The healing controller combines pending candidates into
one DataProvider-scoped plan, pauses ordinary processing, and waits for one operator decision.

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
Output locations and retention policy remain controller configuration; they are not copied into the
persisted operation. Candidate-linked steps identify the uploads whose derived state is invalid.
Retention is enforced while the draft remains mutable; approval freezes the step graph, so later
controller progression does not reject it merely because the configured window advanced.

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

Run each scenario on a fresh baseline:

1. **Inspect, approve, and replay a correction.** Replace one D2 raw object, retain the other
   objects, and write a new `done` generation after the directory is final. Inspect the candidate
   and its `APPROVAL_REQUIRED` plan with the commands above. Verify the stored prior and current
   object generations, the D2 steps for both model lines, and the dependent D3 memoized step.
   Confirm the candidate creates no labeling work before approval. Approve it with `CORRECT` and
   the current plan etag. Leave the raw directory unchanged while the controller evicts and
   replays the approved generation, then recovers D3. Verify that corrected D2 records appear
   once, D3 uses the rebuilt memoized predecessor, and the unaffected direct D3 output is unchanged.
2. **Remove an upload without replacement.** Remove every D2 raw object and write a new `done`
   generation. Inspect the removal candidate and approve it with `NO_REPLACEMENT`. Verify that
   D2 has no replacement output or active metadata, while memoized D3 rebuilds from retained D1.
   The direct D3 output must remain unchanged.
3. **Drain existing work and defer new uploads.** Use supported test controls to keep a labeling
   execution and an availability lease active before publishing and approving a correction that
   affects them. Verify that the operation remains in `DRAINING` and that its output generations
   are not deleted until both finish. Publish another ordinary upload while the fence is held:
   registration may finish, but labeling must remain deferred. Release the test controls and
   observe automatic eviction, replay, recovery, and availability handoff. Verify that the
   deferred upload subsequently completes without rewriting its `done` marker. Do not manually
   pause `DataAvailabilitySync`.
4. **Retry from a durable checkpoint.** On test output only, temporarily hold an exact GCS object
   generation that the plan must delete. Approve the correction and inspect the resulting
   `NEEDS_ATTENTION` operation, its saved resume state, and its completed steps. Release the hold,
   retrieve the current etag, and invoke `retry-correction-plan`. Verify that
   automatic progression resumes from the saved state, preserves completed checkpoints, and
   finishes with one replacement per affected upload/model line. No operator eviction, replay,
   or resume command is part of this procedure.

For every scenario, inspect the operation and replacement model lines separately. An operation
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
