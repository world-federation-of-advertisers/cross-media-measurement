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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import kotlin.time.TimeSource
import org.wfanet.measurement.common.Instrumentation

/**
 * OpenTelemetry metrics for the Results Fulfiller pipeline.
 *
 * Metrics are emitted using the shared EDPA telemetry module and tagged with resource attributes
 * configured during SDK initialization (service name, environment, data provider).
 *
 * @param meter The OpenTelemetry meter to use for creating metrics instruments.
 */
class ResultsFulfillerMetrics(meter: Meter) {
  val requisitionFulfillmentLatency: DoubleHistogram =
    meter
      .histogramBuilder("edpa.results_fulfiller.requisition_fulfillment_latency")
      .setDescription("End-to-end latency per requisition (s)")
      .setUnit("s")
      .build()

  val reportFulfillmentLatency: DoubleHistogram =
    meter
      .histogramBuilder("edpa.results_fulfiller.report_fulfillment_latency")
      .setDescription("End-to-end latency per report (s)")
      .setUnit("s")
      .build()

  val reportProcessingDuration: DoubleHistogram =
    meter
      .histogramBuilder("edpa.results_fulfiller.report_processing_duration")
      .setDescription("Processing duration per report within the fulfiller (s)")
      .setUnit("s")
      .build()

  val requisitionsProcessed: LongCounter =
    meter
      .counterBuilder("edpa.results_fulfiller.requisitions_processed")
      .setDescription("Count of requisitions processed by status")
      .build()

  val requisitionProcessingDuration: DoubleHistogram =
    meter
      .histogramBuilder("edpa.results_fulfiller.requisition_processing_duration")
      .setDescription("Processing duration per requisition within the fulfiller (s)")
      .setUnit("s")
      .build()

  val vidIndexBuildDuration: DoubleHistogram =
    meter
      .histogramBuilder("edpa.results_fulfiller.vid_index_build_duration")
      .setDescription("Duration to build VID index (s)")
      .setUnit("s")
      .build()

  val frequencyVectorDuration: DoubleHistogram =
    meter
      .histogramBuilder("edpa.results_fulfiller.frequency_vector_duration")
      .setDescription("Duration to compute frequency vectors (s)")
      .setUnit("s")
      .build()

  val sendDuration: DoubleHistogram =
    meter
      .histogramBuilder("edpa.results_fulfiller.send_duration")
      .setDescription("Duration to send fulfillment to Kingdom (s)")
      .setUnit("s")
      .build()

  val networkTasksDuration: DoubleHistogram =
    meter
      .histogramBuilder("edpa.results_fulfiller.network_tasks_duration")
      .setDescription("Duration for network operations (s)")
      .setUnit("s")
      .build()

  companion object {
    private val statusKey = AttributeKey.stringKey("edpa.results_fulfiller.status")

    val statusSuccess: Attributes = Attributes.of(statusKey, "success")
    val statusFailure: Attributes = Attributes.of(statusKey, "failure")

    /** Creates a ResultsFulfillerMetrics instance using the default Instrumentation meter. */
    fun create(): ResultsFulfillerMetrics = ResultsFulfillerMetrics(Instrumentation.meter)

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
