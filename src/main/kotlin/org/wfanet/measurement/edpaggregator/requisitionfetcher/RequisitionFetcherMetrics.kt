/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.requisitionfetcher

import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.LongHistogram
import io.opentelemetry.api.metrics.Meter
import org.wfanet.measurement.common.Instrumentation

/**
 * OpenTelemetry metrics for the Requisition Fetcher component.
 *
 * Tracks:
 * - Latency from requisition fetch start to storage completion
 * - Number of requisitions fetched from Kingdom
 * - Number of grouped requisitions written to storage
 * - Number of grouped requisitions that failed to write to storage
 * - Number of per-report worker invocations that failed
 * - Number of times a per-report buffer was split because it hit the size cap
 * - Number of grouped-requisition blobs rebuilt from existing metadata during recovery
 * - Number of `listRequisitions` page-size reductions performed after RESOURCE_EXHAUSTED
 * - Number of STORED groupIds whose recovery was skipped because the full requisition set was not
 *   available in the stream
 * - Peak count of distinct reportIds with open buffers during a single fetch run
 */
class RequisitionFetcherMetrics(meter: Meter = Instrumentation.meter) {

  val fetchLatency: DoubleHistogram =
    meter
      .histogramBuilder("edpa.requisition_fetcher.fetch_latency")
      .setDescription("Latency from requisition fetch start to storage completion")
      .setUnit("s")
      .build()

  val requisitionsFetched: LongCounter =
    meter
      .counterBuilder("edpa.requisition_fetcher.requisitions_fetched")
      .setDescription("Number of requisitions fetched from Kingdom")
      .setUnit("{requisition}")
      .build()

  val storageWrites: LongCounter =
    meter
      .counterBuilder("edpa.requisition_fetcher.storage_writes")
      .setDescription("Number of grouped requisitions written to storage")
      .setUnit("{write}")
      .build()

  val storageFails: LongCounter =
    meter
      .counterBuilder("edpa.requisition_fetcher.storage_fails")
      .setDescription("Number of grouped requisitions that failed to write to storage")
      .setUnit("{failure}")
      .build()

  val reportFailures: LongCounter =
    meter
      .counterBuilder("edpa.requisition_fetcher.report_failures")
      .setDescription("Number of per-report worker invocations that failed")
      .setUnit("{failure}")
      .build()

  val bufferSplits: LongCounter =
    meter
      .counterBuilder("edpa.requisition_fetcher.buffer_splits")
      .setDescription("Number of per-report buffers that were split at the size cap")
      .setUnit("{split}")
      .build()

  val recoveryRebuilds: LongCounter =
    meter
      .counterBuilder("edpa.requisition_fetcher.recovery_rebuilds")
      .setDescription("Number of grouped-requisition blobs rebuilt during recovery")
      .setUnit("{rebuild}")
      .build()

  val pageSizeReductions: LongCounter =
    meter
      .counterBuilder("edpa.requisition_fetcher.page_size_reductions")
      .setDescription(
        "Number of times the listRequisitions page size was halved in response to RESOURCE_EXHAUSTED"
      )
      .setUnit("{reduction}")
      .build()

  val recoverySkippedIncomplete: LongCounter =
    meter
      .counterBuilder("edpa.requisition_fetcher.recovery_skipped_incomplete")
      .setDescription(
        "Number of STORED-but-blob-missing groupIds the run could not rebuild because not all " +
          "expected requisitions were available in the stream"
      )
      .setUnit("{group}")
      .build()

  val openBufferHighWaterMark: LongHistogram =
    meter
      .histogramBuilder("edpa.requisition_fetcher.open_buffer_high_water_mark")
      .ofLongs()
      .setDescription(
        "Peak count of distinct reportIds with open buffers during a single fetch run"
      )
      .setUnit("{buffer}")
      .build()

  val bufferedBytesHighWaterMark: LongHistogram =
    meter
      .histogramBuilder("edpa.requisition_fetcher.buffered_bytes_high_water_mark")
      .ofLongs()
      .setDescription(
        "Peak total serialized bytes across all open buffers during a single fetch run — the " +
          "memory-pressure signal for the fetcher's producer-side buffering, orthogonal to " +
          "open_buffer_high_water_mark (cardinality)"
      )
      .setUnit("By")
      .build()

  companion object {
    val Default = RequisitionFetcherMetrics()
  }
}
