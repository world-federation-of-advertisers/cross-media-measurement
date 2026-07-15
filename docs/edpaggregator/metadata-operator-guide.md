# EDP Aggregator Metadata Operator Guide

Operational reference for the two metadata-heavy components of the EDP Aggregator
pipeline: the **RequisitionFetcher** Cloud Function and the **ImpressionMetadata
service** (public v1alpha API, internal API, and its Spanner backing table). It
covers how they behave at scale, the tuning knobs that matter, the failure modes
to watch for, and a symptom → diagnosis → fix playbook.

This guide assumes familiarity with the deployment mechanics in the
[deployment guide](deployment-guide.md) and the end-to-end request flow in the
[report debugging guide](report-debugging-guide.md). Where those already cover a
topic (config file layout, secret mappings, log locations), this guide links
rather than repeats.

## Contents

- [Component overview](#component-overview)
- [RequisitionFetcher: how it works](#requisitionfetcher-how-it-works)
- [RequisitionFetcher: tuning knobs](#requisitionfetcher-tuning-knobs)
- [RequisitionFetcher: behavior at scale](#requisitionfetcher-behavior-at-scale)
- [ImpressionMetadata service: how it works](#impressionmetadata-service-how-it-works)
- [ImpressionMetadata service: behavior at scale](#impressionmetadata-service-behavior-at-scale)
- [Spanner mutation limits (both paths)](#spanner-mutation-limits-both-paths)
- [Metrics to watch](#metrics-to-watch)
- [Troubleshooting playbook](#troubleshooting-playbook)
- [Quick tuning reference](#quick-tuning-reference)

## Component overview

| Component | Runtime | Backing store | What it does |
|-----------|---------|---------------|--------------|
| RequisitionFetcher | HTTP Cloud Function (gen2), triggered by Cloud Scheduler | Writes GroupedRequisitions blobs to GCS; calls the RequisitionMetadata service | Streams `UNFULFILLED` requisitions from the Kingdom, groups them by report, writes one blob per group, and records `STORED` RequisitionMetadata |
| RequisitionMetadata service | gRPC service (public v1alpha + internal), in the EDPA cluster | Spanner `RequisitionMetadata` + `RequisitionMetadataActions` | Idempotent create/list/refuse/queue of per-requisition metadata; the source of truth for what the fetcher has already persisted and what the results-fulfiller should process |
| ImpressionMetadata service | gRPC service (public v1alpha + internal), in the EDPA cluster | Spanner `ImpressionMetadata` | Records where each EDP's encrypted impression blobs live and the model line / event group / interval they cover; queried by the results-fulfiller and by data-availability |
| data-availability-sync | HTTP Cloud Function, triggered by a `done` blob | Calls the ImpressionMetadata service | Reads `metadata*.binpb` on `done`, upserts ImpressionMetadata, and updates Kingdom data-availability intervals |

The RequisitionFetcher is the write-heavy producer on the requisition side; the
ImpressionMetadata service is the read-heavy lookup on the impression side that
the results-fulfiller depends on. Both are metadata-bound, so both are governed
by the same two forces: **Spanner per-transaction mutation limits** and
**throttled, mostly-serial RPC pacing**.

## RequisitionFetcher: how it works

Each scheduled invocation:

1. Streams `UNFULFILLED` requisitions from the Kingdom through a bounded channel
   into a **single consumer coroutine**. Requisitions are buffered in memory
   keyed strictly by `reportId` (not by `updateTime` — a multi-metric report
   legitimately spans many `updateTime`s, and keying on it would fragment every
   such report into one blob per requisition).
2. Dispatches buffers by two triggers, neither of which is end-of-stream (the
   stream carries no per-report completeness signal and can outlive the instance):
   - **Periodic drain** — every `FLUSH_INTERVAL` (default 5m) a background ticker
     flushes *all* open buffers. This is the primary trigger.
   - **Total-bytes backstop** — when serialized bytes across all open buffers
     reach `MAX_TOTAL_BUFFERED_BYTES` (default 256 MiB), all buffers flush
     immediately, bounding memory between ticks.
3. For each drained report, lists existing RequisitionMetadata for that report,
   filters to the not-yet-recorded requisitions, validates them, splits them into
   groups of at most `MAX_REQUISITIONS_PER_GROUP` (default 1000), and for each
   group **writes the blob first, then creates `STORED` metadata**. Blob-first
   ordering means a crash can only leave a recoverable orphan blob (no metadata),
   never the wedge state (metadata without blob).

A report whose requisitions all arrive in one window becomes a single blob; a
report straddling K drain windows (or a byte-cap flush) is split across ~K blobs.
Splits are counted in the `buffer_splits` metric.

## RequisitionFetcher: tuning knobs

All are environment variables on the Cloud Function (set via the
`requisition_fetcher_env_var` terraform variable), plus two deploy-level knobs on
the function resource (`timeout_seconds`, `max_instances`). Function memory is a
third deploy-level input but is **hardcoded** in the module, not a per-environment
variable (see the memory row).

| Knob | Where | Default | Effect |
|------|-------|---------|--------|
| `FLUSH_INTERVAL` | env var | `5m` | Wall-clock period between forced drains. Must be **less than the function timeout** or the ticker never fires. |
| `MAX_TOTAL_BUFFERED_BYTES` | env var | `268435456` (256 MiB) | Memory backstop across all open buffers (serialized bytes). **Set as a plain integer number of bytes** — it is parsed with `toLongOrNull()`, so a human-readable value like `256MiB` is silently ignored and falls back to the default. Lower it to drain sooner and cap heap; raise it to hold more and split less. |
| `MAX_REQUISITIONS_PER_GROUP` | env var | `1000` | Max requisitions per blob / per metadata `BatchCreate`. Bounds the Spanner mutation count per transaction. |
| `METADATA_REQUEST_INTERVAL` | env var | `100ms` | Minimum interval between RequisitionMetadata service RPCs. The pacing multiplier for all list/batch-create calls. |
| `GRPC_REQUEST_INTERVAL` | env var | `1s` | Minimum interval between Kingdom mutation RPCs (e.g. `refuseRequisition`). |
| `KINGDOM_EVENT_GROUP_REQUEST_INTERVAL` | env var | `50ms` | Minimum interval between Kingdom `getEventGroup` RPCs (called during grouping). |
| `PAGE_SIZE` | env var | `50` | Starting page size for `listRequisitions`. Halved and retried on gRPC `RESOURCE_EXHAUSTED` (surfaced by the `page_size_reductions` metric), down to a floor of 1. |
| function timeout | terraform `timeout_seconds` | `600s` | How long a single invocation may run. **Must exceed `FLUSH_INTERVAL`** so at least one drain happens. Max for an HTTP-triggered gen2 function is **3600s (60m)**. |
| max instances | terraform `max_instances` | `1` | Concurrency cap. `1` prevents overlapping invocations from processing the same `UNFULFILLED` requisitions. |
| function memory | **hardcoded** `--memory=512MB` in `http-cloud-function/main.tf` | `512MB` | Heap ceiling. **Not a terraform variable** — sizing it per environment currently requires editing the module. Must be above the peak buffered working set (see scale section). |

### The timeout / drain / schedule relationship

These three values interact and must be set together:

- **`FLUSH_INTERVAL` < `timeout_seconds`** — otherwise the function is killed
  before the first drain and the periodic-drain mechanism is inert (the whole
  run degrades to "buffer, then final-drain at the end").
- **`timeout_seconds` vs. the Cloud Scheduler interval** — at the default 600s
  timeout under a 15-minute schedule, a run always finishes before the next is
  triggered, so overlap is structurally impossible. If you raise the timeout
  above the schedule interval (e.g. 3600s in production), that no longer holds:
  overlap protection then rests **entirely on `max_instances = 1`** (a busy
  instance makes a concurrent scheduler fire a no-op). In that case, also widen
  the scheduler interval to exceed the expected drain time so it isn't firing
  rejected requests mid-drain.

### Production sizing

The 512MB / 600s defaults suit test environments. A production data provider
with a large `UNFULFILLED` backlog needs more:

- **Timeout**: raise `timeout_seconds` toward **3600s** (the gen2 HTTP maximum)
  so a large backlog can drain in one run.
- **Memory vs. buffer cap**: two dials bound peak heap. `MAX_TOTAL_BUFFERED_BYTES`
  is an env var (lower it to drain sooner, less heap, more splits); function
  `--memory` is currently **hardcoded to 512MB** in the module, so raising it
  per environment requires a module edit (raising it lets buffers hold more with
  fewer splits). Do **not** set `MAX_TOTAL_BUFFERED_BYTES` below a single
  report's working set, or the backstop fires mid-report and refragments — the
  exact failure this design removed. The safe band is: above the largest
  expected single-report working set, below what the heap can hold given
  unpacked-proto overhead.

## RequisitionFetcher: behavior at scale

Worked example: **5,000 `UNFULFILLED` requisitions in one run.** The cost profile
depends heavily on how they distribute across reports, because all metadata work
is per-report.

| Distribution | Reports | Metadata list RPCs | List cost @ 100ms serial |
|--------------|---------|--------------------|--------------------------|
| 5,000 reports × 1 req | 5,000 | ~5,000 | ~500s — problematic |
| ~50 reports × 100 req | ~50 | ~50 | ~5s — fine |
| 10 reports × 500 req | 10 | ~50 (paged) | ~5s — fine |

In practice a report carries many requisitions (tens to hundreds), so the
realistic shape is the middle row: **metadata listing is a non-issue** (~50
throttled RPCs). The costs that actually scale with the 5,000 total are:

1. **RequisitionSpec decryption.** Each requisition's spec is currently decrypted
   twice (once to validate, once to group), all serial on the one consumer. At
   5,000 requisitions that is 10,000 HPKE decryptions where 5,000 would do — the
   dominant CPU cost at scale. Tracked as a follow-up optimization.
2. **Blob and metadata-batch size.** A large report groups into a large blob
   (hundreds of unpacked `Requisition` protos held in memory) and one metadata
   `BatchCreate` of hundreds of rows. Memory is bounded by
   `MAX_TOTAL_BUFFERED_BYTES` / `--memory`; the batch size is bounded by
   `MAX_REQUISITIONS_PER_GROUP` (see mutation limits below).

### Failure modes at scale, and why they are recoverable

| Failure | Trigger | Consequence | Recovery |
|---------|---------|-------------|----------|
| **OOM** | Peak buffered heap exceeds `--memory` (all 5k arrive within one drain window and unpacked size exceeds the serialized cap) | Instance killed mid-run | Requisitions stay `UNFULFILLED`; re-fetched next run. Fix by raising `--memory` or lowering `MAX_TOTAL_BUFFERED_BYTES`. |
| **Timeout** | Run exceeds `timeout_seconds` (large backlog, serial decrypt) | Instance killed mid-drain | Completed groups are consistent; incomplete work re-fetched next run. Fix by raising `timeout_seconds` toward 3600s. |
| **Oversized metadata batch** | A single group exceeds the Spanner mutation limit | `BatchCreate` fails every run → wedge | Prevented by `MAX_REQUISITIONS_PER_GROUP` chunking; do not raise it past the safe bound. |

Every fetcher failure mode is recoverable because `UNFULFILLED` requisitions
remain fetchable and metadata creation is idempotent (deterministic
`request_id`), so a re-run never duplicates. The practical risk is **no forward
progress until tuned**, not data loss.

## ImpressionMetadata service: how it works

The ImpressionMetadata service records, per EDP, where encrypted impression blobs
live and what they cover. It exposes matching **public v1alpha** and **internal**
gRPC surfaces:

- `CreateImpressionMetadata` / `BatchCreateImpressionMetadata`
- `UpdateImpressionMetadata` / `BatchUpdateImpressionMetadata`
- `GetImpressionMetadata` / `ListImpressionMetadata`
- `DeleteImpressionMetadata` / `BatchDeleteImpressionMetadata` (soft delete)
- `ComputeModelLineBounds`

Writes come from **data-availability-sync** (on `done` blobs) and **cleanup**
(on blob deletion). Reads come from the **results-fulfiller** (to locate the
impression blob for a model line + event group + interval) and from
data-availability.

### The Spanner `ImpressionMetadata` table

Columns (14): `DataProviderResourceId`, `ImpressionMetadataId`,
`ImpressionMetadataResourceId`, `CreateRequestId`, `UpdateRequestId`, `BlobUri`,
`BlobTypeUrl`, `EventGroupReferenceId`, `CmmsModelLine`, `IntervalStartTime`,
`IntervalEndTime`, `State`, `CreateTime`, `UpdateTime`.

Indexes:

| Index | Purpose |
|-------|---------|
| `ImpressionMetadataByResourceId` (unique) | Lookup by resource ID |
| `ImpressionMetadataByCreateRequestId` (unique, null-filtered) | Idempotency on create |
| `ImpressionMetadataByBlobUri` (unique) | Lookup / cleanup by exact blob URI |
| `ImpressionMetadataByBlobUriPrefix` (non-unique) | Prefix lookup by blob URI. **Not null-filtered** and `BlobUri` is `NOT NULL`, so it adds an index entry on every create — counts toward create-path mutations. |
| `ImpressionMetadataByUpdateRequestId` (unique, null-filtered) | Idempotency on update. `UpdateRequestId` is NULL on create, so it costs nothing on the create path (only on update). |
| `ImpressionMetadataByListFilterAndPagination` | Backs `ListImpressionMetadata` filtered by model line + event group + interval, and pagination |

Entity keys live in an interleaved child table, `ImpressionMetadataEntityKeys`
(`DataProviderResourceId`, `ImpressionMetadataId`, `EntityType`, `EntityId`),
`INTERLEAVE IN PARENT ImpressionMetadata ON DELETE CASCADE`, with a
`ImpressionMetadataEntityKeysByTypeAndId` index. Each entity key on a row is one
additional interleaved row plus its index entry — factor this into write
mutation cost for entity-keyed metadata, and note it backs the `entity_keys`
list filter.

`ListImpressionMetadata` is backed by `ImpressionMetadataByListFilterAndPagination`
(`DataProviderResourceId`, `CmmsModelLine`, `EventGroupReferenceId`, `State`,
`IntervalStartTime`, `IntervalEndTime`, `CreateTime`,
`ImpressionMetadataResourceId`). The public filter exposes `model_line`,
`event_group_reference_id`, `interval_overlaps`, `blob_uri_prefix`,
`entity_keys`, and `blob_uris` (plus a `show_deleted` flag that governs whether
soft-`DELETED` rows are returned — `State` is an index column, not a caller-set
filter field). A filter that supplies the leading index columns (model line,
then event group, then interval) is a range scan; one that omits them cannot use
the index efficiently.

## ImpressionMetadata service: behavior at scale

- **`BatchCreate` from data-availability-sync** is the write hot path. Like the
  requisition side, each batch is one Spanner read-write transaction, so the
  batch size must stay under the mutation limit (see below). A day's worth of
  metadata for many event groups × dates is created here.
- **`ListImpressionMetadata` from the results-fulfiller** is the read hot path.
  It is paginated and index-backed; keep filters aligned to the index prefix
  (model line, then event group, then interval) so they stay range scans.
- **`ComputeModelLineBounds`** aggregates over a model line's rows to find the
  covered interval. It scans more rows than a point lookup; call it deliberately
  (e.g. once per planning step), not in a tight loop, and expect its cost to grow
  with the number of rows for the model line.
- **Soft deletes** (`DeleteImpressionMetadata` sets `State`, does not remove the
  row). Rows accumulate; a table that only ever soft-deletes grows monotonically.
  Factor this into list-scan cost over time.

## Spanner mutation limits (both paths)

Spanner caps a single read-write transaction at ~80,000 mutations, counting a
mutation per written cell **and** per secondary-index entry. Both batch-create
paths are bounded to stay well under this:

- **RequisitionMetadata**: each requisition writes a base row (~14 columns, 8
  secondary indexes) plus a `RequisitionMetadataActions` row recording the
  `UNSPECIFIED → STORED` transition (~6 columns, 3 indexes) in the same
  transaction — roughly 30 mutations/requisition counting one entry per index,
  up to ~64 counting every index cell. At the `MAX_REQUISITIONS_PER_GROUP`
  default of 1000 that is ~30k–64k mutations, under the limit even in the worst
  case. **Do not raise `MAX_REQUISITIONS_PER_GROUP` without redoing this
  arithmetic** — an oversized group produces a batch that fails every run and
  wedges the report.
- **ImpressionMetadata**: each create writes a base row (14 columns) plus index
  entries. Of the 6 secondary indexes, the 3 non-null-filtered ones
  (`ByResourceId`, `ByBlobUri`, `ByBlobUriPrefix`) and the covering
  `ByListFilterAndPagination` add an entry on every create; the 2 null-filtered
  unique indexes (`ByCreateRequestId`, `ByUpdateRequestId`) cost nothing when
  their key is NULL. That is roughly ~20 mutations/row before entity keys, plus
  **one interleaved `ImpressionMetadataEntityKeys` row + its index entry per
  entity key** on the row. `BatchCreate` size is bounded by the caller
  (data-availability-sync); size the batch so `rows x (~20 + 2 x entity_keys)`
  stays well under the ~80k limit. If you see `BatchCreate` failures citing
  transaction size, reduce the batch size at the caller.

## Metrics to watch

RequisitionFetcher (prefix `edpa.requisition_fetcher.`):

| Metric | Meaning | Watch for |
|--------|---------|-----------|
| `requisitions_fetched` | Requisitions streamed from the Kingdom per run | Sudden drop to 0 with a known backlog → fetcher not seeing work |
| `buffer_splits` | Reports written across more than one blob | A sustained rise → drains firing mid-report (interval too short or buffer cap too low) |
| `open_buffer_high_water_mark` | Peak count of concurrently open report buffers | Growth toward memory pressure |
| `buffered_bytes_high_water_mark` | Peak serialized bytes buffered | Approaching `MAX_TOTAL_BUFFERED_BYTES` routinely → raise memory or lower the cap deliberately |
| `report_failures` (with `error_type`) | Per-report processing failures | A non-transient `error_type` recurring on the same `(data_provider, report_id)` across runs → code bug, will not self-heal |
| `report_refusals` | Reports refused (invalid spec, mixed model lines) | Spikes indicate upstream config problems |
| `recovery_rebuilds` / `recovery_skipped_incomplete` | Blob-recovery outcomes for STORED-but-missing blobs | `recovery_skipped_incomplete` climbing on the same group → a blob is lost and not re-buildable from the current stream |
| `page_size_reductions` | Adaptive `listRequisitions` page-size halvings | Frequent reductions → Kingdom responses near the gRPC message limit |

Check metrics and traces before grepping logs — see the
[report debugging guide](report-debugging-guide.md#telemetry-metrics-and-traces-check-before-grepping-logs).

## Troubleshooting playbook

### Fetcher runs but writes nothing / one blob per requisition

- `requisitions=1` in every `Wrote grouped requisitions blob` log line **for the
  same report in the same run** is fragmentation; across different runs it is
  just the report accumulating over time (normal). Confirm by grouping
  `RequisitionMetadata` by `GroupId` and `Report`: a group with >1 requisition at
  a single `CreateTime` proves grouping is working.
- If genuinely fragmented, check `FLUSH_INTERVAL` is not far shorter than the
  time a report's requisitions take to arrive, and that `MAX_TOTAL_BUFFERED_BYTES`
  is not set below one report's working set.

### Fetcher times out (504) or is killed mid-run

- Large `UNFULFILLED` backlog + serial processing. Raise `timeout_seconds`
  toward 3600s and confirm `FLUSH_INTERVAL` < timeout so drains still fire.
- Confirm it is not a stale-token artifact of a long wait — see the
  [capacity-vs-auth failure mode](report-debugging-guide.md#capacity-exhaustion-masquerading-as-auth-failures).

### Fetcher OOMs

- Peak buffered heap exceeded `--memory`. Either raise `--memory` or lower
  `MAX_TOTAL_BUFFERED_BYTES`. Check `buffered_bytes_high_water_mark` to size it.
  Remember unpacked protos exceed their serialized size, so the heap needs margin
  above the byte cap.

### `BatchCreate` fails citing transaction/mutation size

- A group or batch exceeded the Spanner mutation limit. On the requisition side,
  ensure `MAX_REQUISITIONS_PER_GROUP` is at its safe default (1000) and was not
  raised. On the impression side, reduce the batch size at data-availability-sync.

## Quick tuning reference

| Symptom | First knob to reach for |
|---------|-------------------------|
| Periodic drain never happens | Raise `timeout_seconds` above `FLUSH_INTERVAL` |
| OOM | Lower `MAX_TOTAL_BUFFERED_BYTES` or raise `--memory` |
| Timeout on large backlog | Raise `timeout_seconds` toward 3600s; widen scheduler interval |
| Too many `buffer_splits` | Raise `MAX_TOTAL_BUFFERED_BYTES` / `--memory`, or lengthen `FLUSH_INTERVAL` |
| Metadata RPCs too slow | Lower `METADATA_REQUEST_INTERVAL` (if the service can take the load) |
| Overlapping runs | Ensure `max_instances = 1` |
| `BatchCreate` mutation-limit failure | Keep `MAX_REQUISITIONS_PER_GROUP` at its safe default |
