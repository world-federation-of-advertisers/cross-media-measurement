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
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityMonitor.Companion.EDP_IMPRESSION_PATH_ATTR
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityMonitor.Companion.MODEL_LINE_ATTR

@RunWith(JUnit4::class)
class DataAvailabilityMonitorMetricsTest {

  companion object {
    private const val STALE_DAYS_METRIC = "edpa.data_availability.stale_days"
    private const val GAPS_METRIC = "edpa.data_availability.gaps"
    private const val INCOMPLETE_DATES_METRIC = "edpa.data_availability.zero_impression_dates"
    private const val DATES_WITHOUT_DONE_BLOB_METRIC =
      "edpa.data_availability.dates_without_done_blob"
    private const val MODEL_LINE_NAME = "modelProviders/p1/modelSuites/s1/modelLines/ml1"
    private const val EDP_IMPRESSION_PATH = "edp/edp1/vid-labeled-impressions"
  }

  private data class MetricsTestEnvironment(
    val metrics: DataAvailabilityMonitorMetrics,
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
    val meter = meterProvider.get("data-availability-monitor-test")
    return MetricsTestEnvironment(
      DataAvailabilityMonitorMetrics(meter),
      metricExporter,
      metricReader,
      meterProvider,
    )
  }

  @Test
  fun `staleDaysGauge records stale days`() {
    val metricsEnv = createMetricsEnvironment()
    try {
      metricsEnv.metrics.staleDaysGauge.set(
        5L,
        Attributes.of(MODEL_LINE_ATTR, MODEL_LINE_NAME, EDP_IMPRESSION_PATH_ATTR, EDP_IMPRESSION_PATH),
      )

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(STALE_DAYS_METRIC)
      val point = metricByName.getValue(STALE_DAYS_METRIC).longGaugeData.points.single()
      assertThat(point.value).isEqualTo(5)
      assertThat(point.attributes.get(MODEL_LINE_ATTR)).isEqualTo(MODEL_LINE_NAME)
      assertThat(point.attributes.get(EDP_IMPRESSION_PATH_ATTR)).isEqualTo(EDP_IMPRESSION_PATH)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `gapCounter records gap count`() {
    val metricsEnv = createMetricsEnvironment()
    try {
      metricsEnv.metrics.gapCounter.add(
        3L,
        Attributes.of(MODEL_LINE_ATTR, MODEL_LINE_NAME, EDP_IMPRESSION_PATH_ATTR, EDP_IMPRESSION_PATH),
      )

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(GAPS_METRIC)
      val point = metricByName.getValue(GAPS_METRIC).longSumData.points.single()
      assertThat(point.value).isEqualTo(3)
      assertThat(point.attributes.get(MODEL_LINE_ATTR)).isEqualTo(MODEL_LINE_NAME)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `zeroImpressionDatesCounter records incomplete date count`() {
    val metricsEnv = createMetricsEnvironment()
    try {
      metricsEnv.metrics.zeroImpressionDatesCounter.add(
        2L,
        Attributes.of(MODEL_LINE_ATTR, MODEL_LINE_NAME, EDP_IMPRESSION_PATH_ATTR, EDP_IMPRESSION_PATH),
      )

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(INCOMPLETE_DATES_METRIC)
      val point = metricByName.getValue(INCOMPLETE_DATES_METRIC).longSumData.points.single()
      assertThat(point.value).isEqualTo(2)
      assertThat(point.attributes.get(MODEL_LINE_ATTR)).isEqualTo(MODEL_LINE_NAME)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `datesWithoutDoneBlobCounter records count`() {
    val metricsEnv = createMetricsEnvironment()
    try {
      metricsEnv.metrics.datesWithoutDoneBlobCounter.add(
        1L,
        Attributes.of(MODEL_LINE_ATTR, MODEL_LINE_NAME, EDP_IMPRESSION_PATH_ATTR, EDP_IMPRESSION_PATH),
      )

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(DATES_WITHOUT_DONE_BLOB_METRIC)
      val point =
        metricByName.getValue(DATES_WITHOUT_DONE_BLOB_METRIC).longSumData.points.single()
      assertThat(point.value).isEqualTo(1)
      assertThat(point.attributes.get(MODEL_LINE_ATTR)).isEqualTo(MODEL_LINE_NAME)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `all metrics are recorded together`() {
    val metricsEnv = createMetricsEnvironment()
    try {
      val attrs =
        Attributes.of(MODEL_LINE_ATTR, MODEL_LINE_NAME, EDP_IMPRESSION_PATH_ATTR, EDP_IMPRESSION_PATH)
      metricsEnv.metrics.staleDaysGauge.set(4L, attrs)
      metricsEnv.metrics.gapCounter.add(2L, attrs)
      metricsEnv.metrics.zeroImpressionDatesCounter.add(1L, attrs)
      metricsEnv.metrics.datesWithoutDoneBlobCounter.add(3L, attrs)

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(STALE_DAYS_METRIC)
      assertThat(metricByName).containsKey(GAPS_METRIC)
      assertThat(metricByName).containsKey(INCOMPLETE_DATES_METRIC)
      assertThat(metricByName).containsKey(DATES_WITHOUT_DONE_BLOB_METRIC)

      assertThat(metricByName.getValue(STALE_DAYS_METRIC).longGaugeData.points.single().value)
        .isEqualTo(4)
      assertThat(metricByName.getValue(GAPS_METRIC).longSumData.points.single().value)
        .isEqualTo(2)
      assertThat(
          metricByName.getValue(INCOMPLETE_DATES_METRIC).longSumData.points.single().value
        )
        .isEqualTo(1)
      assertThat(
          metricByName
            .getValue(DATES_WITHOUT_DONE_BLOB_METRIC)
            .longSumData
            .points
            .single()
            .value
        )
        .isEqualTo(3)
    } finally {
      metricsEnv.close()
    }
  }
}
