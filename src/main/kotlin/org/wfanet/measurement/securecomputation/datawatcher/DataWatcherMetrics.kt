package org.wfanet.measurement.securecomputation.datawatcher

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import org.wfanet.measurement.config.securecomputation.WatchedPath

/**
 * Collects metrics for the DataWatcher component.
 *
 * Keeps histogram and counter construction in one place so the watcher logic can stay focused on
 * routing work.
 *
 * @param sinkTypeKey Attribute key for sink type (values come from [WatchedPath.SinkConfigCase]).
 *   Cardinality: bounded by the number of sink config cases.
 * @param queueKey Attribute key for queue name. Cardinality: small, defined by configured queues.
 */
class DataWatcherMetrics(
  meter: Meter,
  private val sinkTypeKey: AttributeKey<String>,
  private val queueKey: AttributeKey<String>,
) {
  private val processingDurationHistogram: DoubleHistogram =
    meter
      .histogramBuilder("edpa.data_watcher.processing_duration")
      .setDescription("Time from regex match to successful sink submission")
      .setUnit("s")
      .build()

  private val queueWritesCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_watcher.queue_writes")
      .setDescription("Number of work items submitted to control plane queue")
      .build()

  fun recordProcessingDuration(config: WatchedPath, durationSeconds: Double) {
    processingDurationHistogram.record(
      durationSeconds,
      Attributes.builder().put(sinkTypeKey, config.sinkConfigCase.name).build(),
    )
  }

  fun recordQueueWrite(config: WatchedPath, queueName: String) {
    queueWritesCounter.add(1, Attributes.builder().put(queueKey, queueName).build())
  }
}
