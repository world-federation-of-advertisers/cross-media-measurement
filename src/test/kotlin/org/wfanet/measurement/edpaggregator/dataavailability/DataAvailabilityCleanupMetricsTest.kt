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

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.common.truth.Truth.assertThat
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityCleanupMetrics.Companion.CLEANUP_STATUS_ATTR
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityCleanupMetrics.Companion.CLEANUP_STATUS_FAILED
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityCleanupMetrics.Companion.CLEANUP_STATUS_SKIPPED
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityCleanupMetrics.Companion.CLEANUP_STATUS_SUCCESS
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityCleanupMetrics.Companion.DATA_PROVIDER_KEY_ATTR
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityCleanupMetrics.Companion.ERROR_TYPE_ATTR
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityCleanupMetrics.Companion.ERROR_TYPE_NOT_FOUND

@RunWith(JUnit4::class)
class DataAvailabilityCleanupMetricsTest {

  companion object {
    private const val CLEANUP_DURATION_METRIC = "edpa.data_availability.cleanup_duration"
    private const val RECORDS_DELETED_METRIC = "edpa.data_availability.records_deleted"
    private const val CLEANUP_ERRORS_METRIC = "edpa.data_availability.cleanup_errors"
    private const val DATA_PROVIDER_NAME = "dataProviders/dataProvider123"
  }

  private data class MetricsTestEnvironment(
    val metrics: DataAvailabilityCleanupMetrics,
    val metricExporter: InMemoryMetricExporter,
    val metricReader: PeriodicMetricReader,
    val meterProvider: SdkMeterProvider,
  ) {
    fun close() {
      meterProvider.close()
    }
  }

  private fun createMetricsEnvironment(): MetricsTestEnvironment {
    val metricExporter = InMemoryMetricExporter.create()
    val metricReader = PeriodicMetricReader.create(metricExporter)
    val meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build()
    val meter = meterProvider.get("data-availability-cleanup-test")
    return MetricsTestEnvironment(
      DataAvailabilityCleanupMetrics(meter),
      metricExporter,
      metricReader,
      meterProvider,
    )
  }

  @Test
  fun `cleanupDurationHistogram records duration with success status`() {
    val metricsEnv = createMetricsEnvironment()
    try {
      metricsEnv.metrics.cleanupDurationHistogram.record(
        1.5,
        Attributes.of(
          DATA_PROVIDER_KEY_ATTR,
          DATA_PROVIDER_NAME,
          CLEANUP_STATUS_ATTR,
          CLEANUP_STATUS_SUCCESS,
        ),
      )

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(CLEANUP_DURATION_METRIC)
      val durationPoint =
        metricByName.getValue(CLEANUP_DURATION_METRIC).histogramData.points.single()
      assertThat(durationPoint.attributes.get(CLEANUP_STATUS_ATTR))
        .isEqualTo(CLEANUP_STATUS_SUCCESS)
      assertThat(durationPoint.attributes.get(DATA_PROVIDER_KEY_ATTR)).isEqualTo(DATA_PROVIDER_NAME)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `cleanupDurationHistogram records duration with failed status`() {
    val metricsEnv = createMetricsEnvironment()
    try {
      metricsEnv.metrics.cleanupDurationHistogram.record(
        0.5,
        Attributes.of(
          DATA_PROVIDER_KEY_ATTR,
          DATA_PROVIDER_NAME,
          CLEANUP_STATUS_ATTR,
          CLEANUP_STATUS_FAILED,
        ),
      )

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(CLEANUP_DURATION_METRIC)
      val durationPoint =
        metricByName.getValue(CLEANUP_DURATION_METRIC).histogramData.points.single()
      assertThat(durationPoint.attributes.get(CLEANUP_STATUS_ATTR)).isEqualTo(CLEANUP_STATUS_FAILED)
      assertThat(durationPoint.attributes.get(DATA_PROVIDER_KEY_ATTR)).isEqualTo(DATA_PROVIDER_NAME)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `cleanupDurationHistogram records duration with skipped status`() {
    val metricsEnv = createMetricsEnvironment()
    try {
      metricsEnv.metrics.cleanupDurationHistogram.record(
        0.2,
        Attributes.of(
          DATA_PROVIDER_KEY_ATTR,
          DATA_PROVIDER_NAME,
          CLEANUP_STATUS_ATTR,
          CLEANUP_STATUS_SKIPPED,
        ),
      )

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(CLEANUP_DURATION_METRIC)
      val durationPoint =
        metricByName.getValue(CLEANUP_DURATION_METRIC).histogramData.points.single()
      assertThat(durationPoint.attributes.get(CLEANUP_STATUS_ATTR))
        .isEqualTo(CLEANUP_STATUS_SKIPPED)
      assertThat(durationPoint.attributes.get(DATA_PROVIDER_KEY_ATTR)).isEqualTo(DATA_PROVIDER_NAME)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `recordsDeletedCounter increments on successful deletion`() {
    val metricsEnv = createMetricsEnvironment()
    try {
      metricsEnv.metrics.recordsDeletedCounter.add(
        1,
        Attributes.of(
          DATA_PROVIDER_KEY_ATTR,
          DATA_PROVIDER_NAME,
          CLEANUP_STATUS_ATTR,
          CLEANUP_STATUS_SUCCESS,
        ),
      )

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(RECORDS_DELETED_METRIC)
      val recordsPoint = metricByName.getValue(RECORDS_DELETED_METRIC).longSumData.points.single()
      assertThat(recordsPoint.value).isEqualTo(1)
      assertThat(recordsPoint.attributes.get(DATA_PROVIDER_KEY_ATTR)).isEqualTo(DATA_PROVIDER_NAME)
      assertThat(recordsPoint.attributes.get(CLEANUP_STATUS_ATTR)).isEqualTo(CLEANUP_STATUS_SUCCESS)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `recordsDeletedCounter accumulates multiple deletions`() {
    val metricsEnv = createMetricsEnvironment()
    try {
      val attributes =
        Attributes.of(
          DATA_PROVIDER_KEY_ATTR,
          DATA_PROVIDER_NAME,
          CLEANUP_STATUS_ATTR,
          CLEANUP_STATUS_SUCCESS,
        )
      metricsEnv.metrics.recordsDeletedCounter.add(1, attributes)
      metricsEnv.metrics.recordsDeletedCounter.add(1, attributes)
      metricsEnv.metrics.recordsDeletedCounter.add(1, attributes)

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(RECORDS_DELETED_METRIC)
      val recordsPoint = metricByName.getValue(RECORDS_DELETED_METRIC).longSumData.points.single()
      assertThat(recordsPoint.value).isEqualTo(3)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `cleanupErrorsCounter increments on not found error`() {
    val metricsEnv = createMetricsEnvironment()
    try {
      metricsEnv.metrics.cleanupErrorsCounter.add(
        1,
        Attributes.of(
          DATA_PROVIDER_KEY_ATTR,
          DATA_PROVIDER_NAME,
          ERROR_TYPE_ATTR,
          ERROR_TYPE_NOT_FOUND,
        ),
      )

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(CLEANUP_ERRORS_METRIC)
      val errorPoint = metricByName.getValue(CLEANUP_ERRORS_METRIC).longSumData.points.single()
      assertThat(errorPoint.value).isEqualTo(1)
      assertThat(errorPoint.attributes.get(DATA_PROVIDER_KEY_ATTR)).isEqualTo(DATA_PROVIDER_NAME)
      assertThat(errorPoint.attributes.get(ERROR_TYPE_ATTR)).isEqualTo(ERROR_TYPE_NOT_FOUND)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `all metrics are recorded together in typical cleanup flow`() {
    val metricsEnv = createMetricsEnvironment()
    try {
      // Simulate a successful cleanup: record duration + record deleted
      metricsEnv.metrics.cleanupDurationHistogram.record(
        0.8,
        Attributes.of(
          DATA_PROVIDER_KEY_ATTR,
          DATA_PROVIDER_NAME,
          CLEANUP_STATUS_ATTR,
          CLEANUP_STATUS_SUCCESS,
        ),
      )
      metricsEnv.metrics.recordsDeletedCounter.add(
        1,
        Attributes.of(
          DATA_PROVIDER_KEY_ATTR,
          DATA_PROVIDER_NAME,
          CLEANUP_STATUS_ATTR,
          CLEANUP_STATUS_SUCCESS,
        ),
      )

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(CLEANUP_DURATION_METRIC)
      assertThat(metricByName).containsKey(RECORDS_DELETED_METRIC)

      val durationPoint =
        metricByName.getValue(CLEANUP_DURATION_METRIC).histogramData.points.single()
      assertThat(durationPoint.attributes.get(CLEANUP_STATUS_ATTR))
        .isEqualTo(CLEANUP_STATUS_SUCCESS)

      val recordsPoint = metricByName.getValue(RECORDS_DELETED_METRIC).longSumData.points.single()
      assertThat(recordsPoint.value).isEqualTo(1)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `metrics recorded together in skipped cleanup flow`() {
    val metricsEnv = createMetricsEnvironment()
    try {
      // Simulate a skipped cleanup (not found): record duration + error
      metricsEnv.metrics.cleanupDurationHistogram.record(
        0.3,
        Attributes.of(
          DATA_PROVIDER_KEY_ATTR,
          DATA_PROVIDER_NAME,
          CLEANUP_STATUS_ATTR,
          CLEANUP_STATUS_SKIPPED,
        ),
      )
      metricsEnv.metrics.cleanupErrorsCounter.add(
        1,
        Attributes.of(
          DATA_PROVIDER_KEY_ATTR,
          DATA_PROVIDER_NAME,
          ERROR_TYPE_ATTR,
          ERROR_TYPE_NOT_FOUND,
        ),
      )

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(CLEANUP_DURATION_METRIC)
      assertThat(metricByName).containsKey(CLEANUP_ERRORS_METRIC)
      assertThat(metricByName).doesNotContainKey(RECORDS_DELETED_METRIC)

      val durationPoint =
        metricByName.getValue(CLEANUP_DURATION_METRIC).histogramData.points.single()
      assertThat(durationPoint.attributes.get(CLEANUP_STATUS_ATTR))
        .isEqualTo(CLEANUP_STATUS_SKIPPED)

      val errorPoint = metricByName.getValue(CLEANUP_ERRORS_METRIC).longSumData.points.single()
      assertThat(errorPoint.value).isEqualTo(1)
      assertThat(errorPoint.attributes.get(ERROR_TYPE_ATTR)).isEqualTo(ERROR_TYPE_NOT_FOUND)
    } finally {
      metricsEnv.close()
    }
  }
}
