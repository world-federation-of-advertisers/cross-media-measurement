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

package org.wfanet.measurement.securecomputation.datawatcher

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.LongCounter
import org.wfanet.measurement.common.Instrumentation

/**
 * OpenTelemetry metrics for the Data Watcher Cloud Function.
 *
 * Metrics:
 * - edpa.data_watcher.match_latency (histogram in seconds)
 * - edpa.data_watcher.queue_writes (counter)
 */
class DataWatcherMetrics(
  meter: io.opentelemetry.api.metrics.Meter = Instrumentation.meter,
) {
  private val matchLatencyHistogram: DoubleHistogram =
    meter
      .histogramBuilder("edpa.data_watcher.match_latency")
      .setDescription("Time from blob match to sink completion")
      .setUnit("s")
      .build()

  private val queueWritesCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_watcher.queue_writes")
      .setDescription("Number of work items submitted to the control plane queue")
      .setUnit("{write}")
      .build()

  fun recordMatchLatency(
    sinkType: String,
    latencySeconds: Double,
  ) {
    matchLatencyHistogram.record(
      latencySeconds,
      Attributes.builder()
        .put(SINK_TYPE_KEY, sinkType)
        .build()
    )
  }

  fun incrementQueueWrites(queueName: String) {
    queueWritesCounter.add(
      1,
      Attributes.builder().put(QUEUE_KEY, queueName).build()
    )
  }

  companion object {
    val instance: DataWatcherMetrics = DataWatcherMetrics()

    private val SINK_TYPE_KEY = AttributeKey.stringKey("sink_type")
    private val QUEUE_KEY = AttributeKey.stringKey("queue")
  }
}
