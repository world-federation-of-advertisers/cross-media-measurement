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

List candidate summaries when the plan needs more investigation:

```shell
vid-labeling-heal list-correction-candidates \
  --data-provider=dataProviders/DATA_PROVIDER \
  --state=PLANNED \
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
  --request-id=UUID \
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

Eviction permanently terminates unfinished availability tasks for obsolete output. A replacement
uses the normal VID-labeling path: VidLabeler writes the versioned output `done` object and records
every pending model-line date without publishing availability work while the eviction fence is
active. When replacement labeling is durable, the operation enters
`AVAILABILITY_HANDOFF_PENDING` and atomically releases the fence. The controller then creates one
exact-generation Data Availability WorkItem per pending model-line date and marks the operation
`COMPLETE` after every WorkItem is durably scheduled or already exists with the same identity. It
does not wait for Data Availability processing itself.

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
  --request-id=UUID \
  --edpa-public-api-target=EDPA_TARGET \
  TLS_FLAGS
```

The command is rejected unless the plan is in `NEEDS_ATTENTION`. The controller resumes from its
durable checkpoints; the CLI never advances individual workflow steps.
