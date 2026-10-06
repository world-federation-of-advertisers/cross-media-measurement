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

Approval freezes the candidate set and decisions. The controller then drains in-flight work,
evicts derived state and output, replays approved replacements, observes normal pipeline recovery,
and releases the DataProvider fence. Operators do not run eviction, replay, or resume commands.

Eviction permanently terminates unfinished availability tasks for obsolete output. A replacement
uses the normal VID-labeling path: VidLabeler writes the versioned output `done` object, creates its
durable availability task, and then completes the model line. Healing completion does not wait for
that task to synchronize because the fence prevents it from acquiring a lease. After the controller
releases the fence, the replacement task acquires its lease and synchronizes availability.

Uploads received while the fence exists are registered with `processing_deferred=true`; they become
eligible only after the plan completes and the fence is released.

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
| `NEEDS_ATTENTION` | A non-retryable failure requires inspection and an explicit retry. |
| `COMPLETE` | Every step completed and the DataProvider fence was released. |

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
