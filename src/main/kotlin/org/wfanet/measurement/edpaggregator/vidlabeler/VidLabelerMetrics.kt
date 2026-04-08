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

package org.wfanet.measurement.edpaggregator.vidlabeler

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import kotlin.time.TimeSource
import org.wfanet.measurement.common.Instrumentation

/**
 * OpenTelemetry metrics for the VID Labeler pipeline.
 *
 * @param meter The OpenTelemetry meter to use for creating metrics instruments.
 */
class VidLabelerMetrics(meter: Meter) {
  val batchLabelingLatency: DoubleHistogram =
    meter
      .histogramBuilder("edpa.vid_labeler.batch_labeling_latency")
      .setDescription("End-to-end latency per batch (s)")
      .setUnit("s")
      .build()

  val readDecryptDuration: DoubleHistogram =
    meter
      .histogramBuilder("edpa.vid_labeler.read_decrypt_duration")
      .setDescription("Duration to read and decrypt raw impressions (s)")
      .setUnit("s")
      .build()

  val modelResolutionDuration: DoubleHistogram =
    meter
      .histogramBuilder("edpa.vid_labeler.model_resolution_duration")
      .setDescription("Duration to resolve VID model lines (s)")
      .setUnit("s")
      .build()

  val inferenceDuration: DoubleHistogram =
    meter
      .histogramBuilder("edpa.vid_labeler.inference_duration")
      .setDescription("Duration to run VID model inference (s)")
      .setUnit("s")
      .build()

  val encryptWriteDuration: DoubleHistogram =
    meter
      .histogramBuilder("edpa.vid_labeler.encrypt_write_duration")
      .setDescription("Duration to encrypt and write labeled impressions (s)")
      .setUnit("s")
      .build()

  val metadataWriteDuration: DoubleHistogram =
    meter
      .histogramBuilder("edpa.vid_labeler.metadata_write_duration")
      .setDescription("Duration to write output metadata (s)")
      .setUnit("s")
      .build()

  val batchesProcessed: LongCounter =
    meter
      .counterBuilder("edpa.vid_labeler.batches_processed")
      .setDescription("Count of batches processed by status")
      .build()

  val impressionsLabeled: LongCounter =
    meter
      .counterBuilder("edpa.vid_labeler.impressions_labeled")
      .setDescription("Count of impressions labeled")
      .build()

  companion object {
    private val statusKey = AttributeKey.stringKey("edpa.vid_labeler.status")

    val statusSuccess: Attributes = Attributes.of(statusKey, "success")
    val statusFailure: Attributes = Attributes.of(statusKey, "failure")

    /** Creates a VidLabelerMetrics instance using the default Instrumentation meter. */
    fun create(): VidLabelerMetrics = VidLabelerMetrics(Instrumentation.meter)

    /**
     * Measures the execution time of a block and records it to the histogram.
     *
     * @param block The code block to measure
     * @return The result of the block execution
     */
    inline fun <T> DoubleHistogram.measured(block: () -> T): T {
      val timer = TimeSource.Monotonic.markNow()
      return try {
        block()
      } finally {
        this.record(timer.elapsedNow().inWholeNanoseconds / 1_000_000_000.0)
      }
    }
  }
}
