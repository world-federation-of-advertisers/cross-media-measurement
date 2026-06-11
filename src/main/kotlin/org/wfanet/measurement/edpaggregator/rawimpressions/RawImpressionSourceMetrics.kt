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

package org.wfanet.measurement.edpaggregator.rawimpressions

import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import org.wfanet.measurement.common.Instrumentation

/**
 * Encapsulates the OpenTelemetry instruments used by [RawImpressionSource].
 *
 * These integrate with `EdpaTelemetry`, so read/drop/emit rates and per-file processing latency
 * surface on operator dashboards (the component runs in the same TEE apps as the rest of the EDPA
 * pipeline).
 *
 * @param meter the OpenTelemetry [Meter] used to create instruments.
 */
class RawImpressionSourceMetrics(meter: Meter = Instrumentation.meter) {
  /** Counter for the number of raw-impression rows read from storage. */
  val rowsReadCounter: LongCounter =
    meter
      .counterBuilder("edpa.raw_impression_source.rows_read")
      .setDescription("Number of raw-impression rows read")
      .build()

  /** Counter for rows dropped because their event-id digest belongs to another shard. */
  val rowsDroppedOtherShardCounter: LongCounter =
    meter
      .counterBuilder("edpa.raw_impression_source.rows_dropped_other_shard")
      .setDescription("Number of rows dropped because they belong to another shard")
      .build()

  /** Counter for shard-surviving rows emitted to the sinks. */
  val rowsEmittedCounter: LongCounter =
    meter
      .counterBuilder("edpa.raw_impression_source.rows_emitted")
      .setDescription("Number of shard-surviving rows emitted to sinks")
      .build()

  /** Histogram of the wall time to read + process one input file, in seconds. */
  val fileProcessingDurationHistogram: DoubleHistogram =
    meter
      .histogramBuilder("edpa.raw_impression_source.file_processing_duration")
      .setDescription("Wall time to read and fully process one raw-impression file")
      .setUnit("s")
      .build()
}
