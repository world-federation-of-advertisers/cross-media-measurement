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

  companion object {
    val Default = RequisitionFetcherMetrics()
  }
}
