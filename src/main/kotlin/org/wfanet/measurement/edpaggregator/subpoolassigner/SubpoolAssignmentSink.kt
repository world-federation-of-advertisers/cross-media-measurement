/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.subpoolassigner

import java.util.concurrent.atomic.AtomicLong
import org.wfanet.measurement.edpaggregator.rawimpressions.LabelerInputMapper
import org.wfanet.measurement.edpaggregator.rawimpressions.ParquetDigestedEvent
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionSource
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow

/**
 * Phase-0 [RawImpressionSource.BlobSink] that turns shard-filtered raw impressions into (subpool,
 * fingerprint) entries in a shared [SubpoolFingerprintsAccumulator].
 *
 * For each event in a batch it:
 * 1. projects the parquet row into a `LabelerInput` via [mapper],
 * 2. drops it if its event timestamp falls outside the model line's [activeWindow] (the per-model-
 *    line time filter the reader deliberately leaves to the sink),
 * 3. runs [labeler] in pool-emit mode to resolve the subpool offset(s), and
 * 4. records `(subpool, fingerprint)` for each offset, keyed by the event's `EventIdDigest`.
 *
 * The accumulator, mapper and labeler are shared and concurrency-safe, so one stateless sink
 * instance can back every blob in a `streamBlobs` run; [processBatch] is invoked concurrently.
 *
 * Per-event outcomes are recorded two ways: as OpenTelemetry counters on [metrics] (for operator
 * dashboards) and as the in-process [labeled] / [droppedOutsideWindow] / [unrouted] tallies, which
 * [SubpoolAssigner] reads after the stream completes for the completion log line and the run
 * summary ([SubpoolAssigner.Result]) — OTel counters are not readable in-process.
 */
class SubpoolAssignmentSink(
  private val mapper: LabelerInputMapper,
  private val labeler: PoolEmitLabeler,
  private val accumulator: SubpoolFingerprintsAccumulator,
  private val activeWindow: ActiveWindow,
  private val metrics: SubpoolAssignerMetrics = SubpoolAssignerMetrics(),
) : RawImpressionSource.BlobSink<ParquetDigestedEvent> {
  private val labeledCounter = AtomicLong()
  private val droppedOutsideWindowCounter = AtomicLong()
  private val unroutedCounter = AtomicLong()
  private val maxTimestampUsecValue = AtomicLong(Long.MIN_VALUE)

  /** Events that routed to at least one subpool. */
  val labeled: Long
    get() = labeledCounter.get()

  /**
   * Latest event timestamp (epoch microseconds) among the events that routed to a subpool, or
   * `null` if none did. [SubpoolAssigner] reduces this with the other shards' values into the
   * `max_event_date` it reports on `MarkPoolAssignmentJobSucceeded`; the rank-map retention sweep
   * later ages out blobs against it. Tracked over labeled (written) events only, since those are
   * the impressions whose fingerprints land in the `SubpoolFingerprints` blobs.
   */
  val maxTimestampUsec: Long?
    get() = maxTimestampUsecValue.get().let { if (it == Long.MIN_VALUE) null else it }

  /** Events dropped because their timestamp was unset or outside the active window. */
  val droppedOutsideWindow: Long
    get() = droppedOutsideWindowCounter.get()

  /** Events the labeler routed to no subpool (hash-fallback path). */
  val unrouted: Long
    get() = unroutedCounter.get()

  override suspend fun processBatch(events: List<ParquetDigestedEvent>) {
    // Tally this batch's outcomes in local vars, then fold them into the shared atomics /
    // OpenTelemetry counters ONCE per batch instead of once per event. The final totals (read after
    // the whole stream drains) and the cumulative counter sums are identical — only the update
    // granularity changes (maxOf is associative, so the per-batch max reduces to the global max).
    var labeledInBatch = 0L
    var droppedInBatch = 0L
    var unroutedInBatch = 0L
    var maxTimestampInBatch = Long.MIN_VALUE
    for (event in events) {
      val input = mapper.project(event.row)
      if (!input.hasTimestampUsec() || !activeWindow.contains(input.timestampUsec)) {
        droppedInBatch++
        continue
      }
      // Hand each primitive pool offset straight to the accumulator (no boxed List<Long>); the
      // return count says whether the event routed anywhere.
      val routed =
        labeler.emit(input) { offset ->
          accumulator.add(offset, event.digest.high, event.digest.low)
        }
      if (routed == 0) {
        // Per-event logging is intentionally avoided on this hot path (potentially billions of
        // events); the aggregate [unrouted] tally and OTel counter carry the signal instead.
        unroutedInBatch++
        continue
      }
      labeledInBatch++
      if (input.timestampUsec > maxTimestampInBatch) {
        maxTimestampInBatch = input.timestampUsec
      }
    }
    if (droppedInBatch > 0L) {
      droppedOutsideWindowCounter.addAndGet(droppedInBatch)
      metrics.eventsDroppedOutsideWindowCounter.add(droppedInBatch)
    }
    if (unroutedInBatch > 0L) {
      unroutedCounter.addAndGet(unroutedInBatch)
      metrics.eventsUnroutedCounter.add(unroutedInBatch)
    }
    if (labeledInBatch > 0L) {
      labeledCounter.addAndGet(labeledInBatch)
      metrics.eventsLabeledCounter.add(labeledInBatch)
    }
    if (maxTimestampInBatch != Long.MIN_VALUE) {
      maxTimestampUsecValue.accumulateAndGet(maxTimestampInBatch, ::maxOf)
    }
  }

  /**
   * No-op. Phase-0 produces no per-blob artifact to finalize: every fingerprint is written into the
   * shared [SubpoolFingerprintsAccumulator], which [SubpoolAssigner] snapshots and uploads exactly
   * once after the whole stream drains. The method exists only to satisfy the
   * [RawImpressionSource.BlobSink] contract, whose `commit()` finalizes per-blob output for sinks
   * that have one (e.g. the Phase-2 streaming RecordIO writer).
   */
  override suspend fun commit() {}

  /**
   * No-op. This sink holds no per-blob resources to release — the only state is the shared
   * accumulator, which outlives any single blob and is owned by [SubpoolAssigner]. The method
   * exists only to satisfy the [RawImpressionSource.BlobSink] contract.
   */
  override suspend fun close() {}
}
