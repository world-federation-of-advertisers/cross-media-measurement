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
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityMonitor.Companion.MODEL_LINE_ATTR
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityMonitor.Companion.EDP_IMPRESSION_PATH_ATTR
import com.google.protobuf.ByteString
import java.io.File
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class DataAvailabilityMonitorTest {

  @get:Rule val tempFolder = TemporaryFolder()

  companion object {
    private const val EDP_IMPRESSION_PATH = "edp/meta/vid-labeled-impressions"
    private val MODEL_LINE_A = ModelLineKey("provider1", "suite1", "modelLineA")
    private val MODEL_LINE_B = ModelLineKey("provider1", "suite1", "modelLineB")
    private val TODAY = LocalDate.of(2026, 3, 15)

    private const val STALE_DAYS_METRIC = "edpa.data_availability.stale_days"
    private const val GAPS_METRIC = "edpa.data_availability.gaps"
    private const val INCOMPLETE_DATES_METRIC = "edpa.data_availability.incomplete_dates"
    private const val DATES_WITHOUT_DONE_BLOB_METRIC =
      "edpa.data_availability.dates_without_done_blob"
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

  private fun createDoneBlob(
    storageClient: FileSystemStorageClient,
    modelLine: String,
    date: String,
  ): Unit = runBlocking {
    val path = "$EDP_IMPRESSION_PATH/model-line/$modelLine/$date/done"
    storageClient.writeBlob(path, ByteString.copyFromUtf8("done"))
  }

  private fun createDataFile(
    storageClient: FileSystemStorageClient,
    modelLine: String,
    date: String,
  ): Unit = runBlocking {
    val path = "$EDP_IMPRESSION_PATH/model-line/$modelLine/$date/data_campaign_1"
    storageClient.writeBlob(path, ByteString.copyFromUtf8("data"))
  }

  private fun createStorageClient(): FileSystemStorageClient {
    return FileSystemStorageClient(tempFolder.root)
  }

  private fun ensureDirectories(modelLine: String, date: String) {
    val dir = File(tempFolder.root, "$EDP_IMPRESSION_PATH/model-line/$modelLine/$date")
    dir.mkdirs()
  }

  // --- check() tests (both staleness and gaps) ---

  @Test
  fun `check returns no issues when all dates are present and recent`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in 12..15) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isFalse()
    assertThat(result.statuses).hasSize(1)

    val status = result.statuses.single()
    assertThat(status.modelLineKey).isEqualTo(MODEL_LINE_A)
    assertThat(status.isStale).isFalse()
    assertThat(status.missingDates).isEmpty()
    assertThat(status.incompleteDates).isEmpty()
    assertThat(status.datesWithoutDoneBlob).isEmpty()
    assertThat(status.latestDate).isEqualTo(LocalDate.of(2026, 3, 15))
    assertThat(status.staleDays).isEqualTo(0)
  }

  @Test
  fun `check detects staleness when latest upload is too old`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in 9..11) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.isStale).isTrue()
    assertThat(status.staleDays).isEqualTo(4)
    assertThat(status.latestDate).isEqualTo(LocalDate.of(2026, 3, 11))
  }

  @Test
  fun `check detects gap in uploaded dates`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in listOf(12, 13, 15)) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.isStale).isFalse()
    assertThat(status.missingDates).containsExactly(LocalDate.of(2026, 3, 14))
  }

  @Test
  fun `check detects multiple gaps`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in listOf(10, 12, 15)) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.missingDates)
      .containsExactly(
        LocalDate.of(2026, 3, 11),
        LocalDate.of(2026, 3, 13),
        LocalDate.of(2026, 3, 14),
      )
  }

  @Test
  fun `check handles multiple model lines independently`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in 13..15) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }
    for (day in 9..11) {
      ensureDirectories(MODEL_LINE_B.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_B.modelLineId, "2026-03-%02d".format(day))
      createDataFile(storageClient, MODEL_LINE_B.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A, MODEL_LINE_B),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isTrue()

    val statusA = result.statuses.first { it.modelLineKey == MODEL_LINE_A }
    assertThat(statusA.isStale).isFalse()
    assertThat(statusA.missingDates).isEmpty()

    val statusB = result.statuses.first { it.modelLineKey == MODEL_LINE_B }
    assertThat(statusB.isStale).isTrue()
    assertThat(statusB.staleDays).isEqualTo(4)
  }

  @Test
  fun `check throws when model line has no uploads`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    assertFailsWith<IllegalArgumentException> { monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY }) }
  }

  @Test
  fun `check does not flag staleness when within threshold`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in 10..12) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isFalse()

    val status = result.statuses.single()
    assertThat(status.isStale).isFalse()
    assertThat(status.staleDays).isEqualTo(3)
  }

  @Test
  fun `check detects both staleness and gaps simultaneously`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in listOf(8, 10)) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.isStale).isTrue()
    assertThat(status.staleDays).isEqualTo(5)
    assertThat(status.missingDates).containsExactly(LocalDate.of(2026, 3, 9))
  }

  // --- checkGaps() tests ---

  @Test
  fun `checkGaps returns no issues when all dates are contiguous`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in 12..15) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      val dataPath =
        "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/2026-03-%02d/metadata_campaign_1.json"
          .format(day)
      storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkGaps()
    assertThat(result.hasIssues).isFalse()

    val status = result.statuses.single()
    assertThat(status.missingDates).isEmpty()
    assertThat(status.incompleteDates).isEmpty()
    assertThat(status.datesWithoutDoneBlob).isEmpty()
    assertThat(status.isStale).isNull()
    assertThat(status.staleDays).isNull()
  }

  @Test
  fun `checkGaps detects missing dates`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in listOf(12, 15)) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      val dataPath =
        "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/2026-03-%02d/metadata_campaign_1.json"
          .format(day)
      storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkGaps()
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.missingDates)
      .containsExactly(LocalDate.of(2026, 3, 13), LocalDate.of(2026, 3, 14))
    assertThat(status.isStale).isNull()
  }

  @Test
  fun `checkGaps throws when model line has no uploads`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    assertFailsWith<IllegalArgumentException> { monitor.checkGaps() }
  }

  // --- Edge case tests ---

  @Test
  fun `checkGaps detects empty date folder with done blob but no data files`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    // March 13 has done + data, March 14 has only done (empty), March 15 has done + data
    for (day in listOf(13, 14, 15)) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }
    // Add data files for March 13 and 15 only
    for (day in listOf(13, 15)) {
      val dataPath =
        "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/2026-03-%02d/metadata_campaign_1.json"
          .format(day)
      storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkGaps()
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.missingDates).isEmpty()
    assertThat(status.incompleteDates).containsExactly(LocalDate.of(2026, 3, 14))
  }

  @Test
  fun `checkGaps returns no issues when all folders have data files`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    for (day in 13..15) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      val dataPath =
        "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/2026-03-%02d/metadata_campaign_1.json"
          .format(day)
      storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkGaps()
    assertThat(result.hasIssues).isFalse()
    assertThat(result.statuses.single().incompleteDates).isEmpty()
  }


  @Test
  fun `checkFullStatus throws when folder name is not a valid date`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    // Valid date folder
    ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-15")
    createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")
    createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")

    // Invalid date format folder
    val badPath = "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/not-a-date/done"
    File(tempFolder.root, badPath).parentFile.mkdirs()
    storageClient.writeBlob(badPath, ByteString.copyFromUtf8("done"))

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    assertFailsWith<java.time.format.DateTimeParseException> {
      monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    }
  }

  @Test
  fun `checkGaps throws when folder name is not a valid date`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    // Valid date folder
    ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-15")
    createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")

    // Invalid date format folder
    val badPath = "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/not-a-date/done"
    File(tempFolder.root, badPath).parentFile.mkdirs()
    storageClient.writeBlob(badPath, ByteString.copyFromUtf8("done"))

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    assertFailsWith<java.time.format.DateTimeParseException> {
      monitor.checkGaps()
    }
  }

  @Test
  fun `checkFullStatus detects empty dates with done blob but no data files`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    // March 13 has done + data, March 14 has only done (empty), March 15 has done + data
    for (day in listOf(13, 14, 15)) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }
    for (day in listOf(13, 15)) {
      createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.missingDates).isEmpty()
    assertThat(status.incompleteDates).containsExactly(LocalDate.of(2026, 3, 14))
  }

  @Test
  fun `checkFullStatus throws when maxStaleDays is zero`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-15")
    createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")
    createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    assertFailsWith<IllegalArgumentException> {
      monitor.checkFullStatus(maxStaleDays = 0, clock = { TODAY })
    }
  }

  @Test
  fun `constructor throws when activeModelLines is empty`() {
    val storageClient = createStorageClient()

    assertFailsWith<IllegalArgumentException> {
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = emptySet(),
      )
    }
  }

  @Test
  fun `checkFullStatus returns no gaps when only one date exists`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-15")
    createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")
    createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isFalse()

    val status = result.statuses.single()
    assertThat(status.missingDates).isEmpty()
    assertThat(status.incompleteDates).isEmpty()
    assertThat(status.datesWithoutDoneBlob).isEmpty()
    assertThat(status.staleDays).isEqualTo(0)
  }

  @Test
  fun `checkGaps returns no gaps when only one date exists`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-15")
    createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")
    createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkGaps()
    assertThat(result.hasIssues).isFalse()

    val status = result.statuses.single()
    assertThat(status.missingDates).isEmpty()
    assertThat(status.incompleteDates).isEmpty()
    assertThat(status.datesWithoutDoneBlob).isEmpty()
    assertThat(status.isStale).isNull()
    assertThat(status.staleDays).isNull()
  }

  @Test
  fun `checkFullStatus detects date folders without done blob`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    // March 13 and 15 have done + data, March 14 has data but no done blob
    for (day in listOf(13, 14, 15)) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }
    for (day in listOf(13, 15)) {
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.datesWithoutDoneBlob).containsExactly(LocalDate.of(2026, 3, 14))
    assertThat(status.missingDates).containsExactly(LocalDate.of(2026, 3, 14))
    assertThat(status.incompleteDates).isEmpty()
  }

  @Test
  fun `checkGaps detects date folders without done blob`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    // March 13 and 15 have done + data, March 14 has data but no done blob
    for (day in listOf(13, 14, 15)) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }
    for (day in listOf(13, 15)) {
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkGaps()
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.datesWithoutDoneBlob).containsExactly(LocalDate.of(2026, 3, 14))
    assertThat(status.missingDates).containsExactly(LocalDate.of(2026, 3, 14))
    assertThat(status.incompleteDates).isEmpty()
  }

  @Test
  fun `checkFullStatus reports all metrics together`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    // March 10: done + data (good)
    ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-10")
    createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-10")
    createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-10")

    // March 11: gap (missing entirely)

    // March 12: done but no data (incomplete)
    ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-12")
    createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-12")

    // March 13: data but no done blob (datesWithoutDoneBlob)
    ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-13")
    createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-13")

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    // Stale: latest done date is March 12, today is March 15 = 3 days, not > 3
    // But gaps and other issues exist
    assertThat(status.staleDays).isEqualTo(3)
    assertThat(status.isStale).isFalse()
    assertThat(status.missingDates).containsExactly(LocalDate.of(2026, 3, 11))
    assertThat(status.incompleteDates).containsExactly(LocalDate.of(2026, 3, 12))
    assertThat(status.datesWithoutDoneBlob).containsExactly(LocalDate.of(2026, 3, 13))
  }

  @Test
  fun `checkFullStatus emits stale days and gap metrics`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    val metricsEnv = createMetricsEnvironment()
    try {
      // Create dates with a gap: 10, 12 (missing 11), stale by 5 days
      for (day in listOf(8, 10)) {
        ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
        createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
        createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      }

      val monitor =
        DataAvailabilityMonitor(
          storageClient = storageClient,
          edpImpressionPath = EDP_IMPRESSION_PATH,
          activeModelLines = setOf(MODEL_LINE_A),
          metrics = metricsEnv.metrics,
        )

      monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(STALE_DAYS_METRIC)
      val stalePoint = metricByName.getValue(STALE_DAYS_METRIC).longGaugeData.points.single()
      assertThat(stalePoint.value).isEqualTo(5)
      assertThat(stalePoint.attributes.get(MODEL_LINE_ATTR)).isEqualTo(MODEL_LINE_A.toName())

      assertThat(metricByName).containsKey(GAPS_METRIC)
      val gapPoint = metricByName.getValue(GAPS_METRIC).longSumData.points.single()
      assertThat(gapPoint.value).isEqualTo(1)
      assertThat(gapPoint.attributes.get(MODEL_LINE_ATTR)).isEqualTo(MODEL_LINE_A.toName())
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `checkFullStatus emits all metrics for combined issues`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    val metricsEnv = createMetricsEnvironment()
    try {
      // March 10: done + data (good)
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-10")
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-10")
      createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-10")

      // March 11: gap (missing entirely)

      // March 12: done but no data (incomplete)
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-12")
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-12")

      // March 13: data but no done blob (datesWithoutDoneBlob)
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-13")
      createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-13")

      val monitor =
        DataAvailabilityMonitor(
          storageClient = storageClient,
          edpImpressionPath = EDP_IMPRESSION_PATH,
          activeModelLines = setOf(MODEL_LINE_A),
          metrics = metricsEnv.metrics,
        )

      monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(STALE_DAYS_METRIC)
      assertThat(metricByName.getValue(STALE_DAYS_METRIC).longGaugeData.points.single().value)
        .isEqualTo(3)

      assertThat(metricByName).containsKey(GAPS_METRIC)
      assertThat(metricByName.getValue(GAPS_METRIC).longSumData.points.single().value)
        .isEqualTo(1)

      assertThat(metricByName).containsKey(INCOMPLETE_DATES_METRIC)
      assertThat(
          metricByName.getValue(INCOMPLETE_DATES_METRIC).longSumData.points.single().value
        )
        .isEqualTo(1)

      assertThat(metricByName).containsKey(DATES_WITHOUT_DONE_BLOB_METRIC)
      assertThat(
          metricByName
            .getValue(DATES_WITHOUT_DONE_BLOB_METRIC)
            .longSumData
            .points
            .single()
            .value
        )
        .isEqualTo(1)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `checkGaps emits gap metrics`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    val metricsEnv = createMetricsEnvironment()
    try {
      for (day in listOf(12, 15)) {
        ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
        createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
        createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      }

      val monitor =
        DataAvailabilityMonitor(
          storageClient = storageClient,
          edpImpressionPath = EDP_IMPRESSION_PATH,
          activeModelLines = setOf(MODEL_LINE_A),
          metrics = metricsEnv.metrics,
        )

      monitor.checkGaps()

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      assertThat(metricByName).containsKey(GAPS_METRIC)
      val gapPoint = metricByName.getValue(GAPS_METRIC).longSumData.points.single()
      assertThat(gapPoint.value).isEqualTo(2)
      assertThat(gapPoint.attributes.get(MODEL_LINE_ATTR)).isEqualTo(MODEL_LINE_A.toName())
      assertThat(gapPoint.attributes.get(EDP_IMPRESSION_PATH_ATTR)).isEqualTo(EDP_IMPRESSION_PATH)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `checkFullStatus does not emit gap or issue metrics when healthy`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    val metricsEnv = createMetricsEnvironment()
    try {
      for (day in 13..15) {
        ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
        createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
        createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      }

      val monitor =
        DataAvailabilityMonitor(
          storageClient = storageClient,
          edpImpressionPath = EDP_IMPRESSION_PATH,
          activeModelLines = setOf(MODEL_LINE_A),
          metrics = metricsEnv.metrics,
        )

      monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      // staleDays gauge should still be emitted (value 0)
      assertThat(metricByName).containsKey(STALE_DAYS_METRIC)
      assertThat(metricByName.getValue(STALE_DAYS_METRIC).longGaugeData.points.single().value)
        .isEqualTo(0)

      // No gaps, incomplete, or missing done blob metrics should be emitted
      assertThat(metricByName).doesNotContainKey(GAPS_METRIC)
      assertThat(metricByName).doesNotContainKey(INCOMPLETE_DATES_METRIC)
      assertThat(metricByName).doesNotContainKey(DATES_WITHOUT_DONE_BLOB_METRIC)
    } finally {
      metricsEnv.close()
    }
  }
}
