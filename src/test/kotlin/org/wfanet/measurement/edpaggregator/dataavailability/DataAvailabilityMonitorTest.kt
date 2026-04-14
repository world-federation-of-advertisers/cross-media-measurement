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
import com.google.protobuf.ByteString
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.metrics.export.MetricReader
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter
import java.io.File
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityMonitor.Companion.EDP_IMPRESSION_PATH_ATTR
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityMonitor.Companion.MODEL_LINE_ATTR
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata as V1AlphaImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata as v1alphaImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse

@RunWith(JUnit4::class)
class DataAvailabilityMonitorTest {

  @get:Rule val tempFolder = TemporaryFolder()

  private val DATA_PROVIDER_NAME = "dataProviders/testProvider1"
  private val BUCKET_NAME = "test-bucket"

  private val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { listImpressionMetadata(org.mockito.kotlin.any<ListImpressionMetadataRequest>()) }
        .thenAnswer { _ ->
          listImpressionMetadataResponse {
            impressionMetadata += v1alphaImpressionMetadata {
              name = "$DATA_PROVIDER_NAME/impressionMetadata/imp-deleted-1"
              blobUri = "gs://$BUCKET_NAME/$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/2026-03-10/metadata_campaign_123.json"
              state = V1AlphaImpressionMetadata.State.DELETED
            }
          }
        }
    }

  private val noDeletedEntriesServiceMock: ImpressionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { listImpressionMetadata(org.mockito.kotlin.any<ListImpressionMetadataRequest>()) }
        .thenAnswer { _ ->
          listImpressionMetadataResponse {}
        }
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(impressionMetadataServiceMock) }

  @get:Rule
  val grpcTestServerRule2 = GrpcTestServerRule { addService(noDeletedEntriesServiceMock) }

  private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub by lazy {
    ImpressionMetadataServiceCoroutineStub(grpcTestServerRule.channel)
  }

  private val noDeletedEntriesStub: ImpressionMetadataServiceCoroutineStub by lazy {
    ImpressionMetadataServiceCoroutineStub(grpcTestServerRule2.channel)
  }


  companion object {
    private const val EDP_IMPRESSION_PATH = "edp/edp1/vid-labeled-impressions"
    private val MODEL_LINE_A = ModelLineKey("provider1", "suite1", "modelLineA")
    private val MODEL_LINE_B = ModelLineKey("provider1", "suite1", "modelLineB")
    private val TODAY = LocalDate.of(2026, 3, 15)

    private const val STALE_DAYS_METRIC = "edpa.data_availability.stale_days"
    private const val DATE_COUNT_METRIC = "edpa.data_availability.date_count"
  }

  private lateinit var openTelemetry: OpenTelemetrySdk
  private lateinit var metricExporter: InMemoryMetricExporter
  private lateinit var metricReader: MetricReader

  @Before
  fun initTelemetry() {
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    metricExporter = InMemoryMetricExporter.create()
    metricReader = PeriodicMetricReader.create(metricExporter)
    openTelemetry =
      OpenTelemetrySdk.builder()
        .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(metricReader).build())
        .buildAndRegisterGlobal()
  }

  @After
  fun cleanupTelemetry() {
    if (this::openTelemetry.isInitialized) {
      openTelemetry.close()
    }
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
  }

  private fun collectMetrics(): List<MetricData> {
    metricReader.forceFlush()
    return metricExporter.finishedMetricItems
  }

  private fun getDateStatusCount(metrics: List<MetricData>, status: String): Long? {
    val dateCountMetric = metrics.find { it.name == DATE_COUNT_METRIC } ?: return null
    return dateCountMetric.longSumData.points
      .find { it.attributes.get(DataAvailabilityMonitorMetrics.DATE_STATUS_ATTR) == status }
      ?.value
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

  private fun getFile(modelLine: String, date: String, fileName: String): File {
    return File(tempFolder.root, "$EDP_IMPRESSION_PATH/model-line/$modelLine/$date/$fileName")
  }

  @Test
  fun `check returns no issues when all dates are present and recent`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in 12..15) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.statuses).hasSize(1)

    val status = result.statuses.single()
    assertThat(status.modelLineKey).isEqualTo(MODEL_LINE_A)
    assertThat(status.isStale).isFalse()
    assertThat(status.gapDates).isEmpty()
    assertThat(status.zeroImpressionDates).isEmpty()
    assertThat(status.datesWithoutDoneBlob).isEmpty()
    assertThat(status.healthyDates)
      .containsExactly(
        LocalDate.of(2026, 3, 12),
        LocalDate.of(2026, 3, 13),
        LocalDate.of(2026, 3, 14),
        LocalDate.of(2026, 3, 15),
      )
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

    val status = result.statuses.single()
    assertThat(status.isStale).isFalse()
    assertThat(status.gapDates).containsExactly(LocalDate.of(2026, 3, 14))
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

    val status = result.statuses.single()
    assertThat(status.gapDates)
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

    val statusA = result.statuses.first { it.modelLineKey == MODEL_LINE_A }
    assertThat(statusA.isStale).isFalse()
    assertThat(statusA.gapDates).isEmpty()

    val statusB = result.statuses.first { it.modelLineKey == MODEL_LINE_B }
    assertThat(statusB.isStale).isTrue()
    assertThat(statusB.staleDays).isEqualTo(4)
  }

  @Test
  fun `check returns null fields when model line has no uploads`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    val status = result.statuses.single()
    assertThat(status.isStale).isNull()
    assertThat(status.gapDates).isNull()
    assertThat(status.staleDays).isNull()
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

    val status = result.statuses.single()
    assertThat(status.isStale).isTrue()
    assertThat(status.staleDays).isEqualTo(5)
    assertThat(status.gapDates).containsExactly(LocalDate.of(2026, 3, 9))
  }

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

    val status = result.statuses.single()
    assertThat(status.gapDates).isEmpty()
    assertThat(status.zeroImpressionDates).isEmpty()
    assertThat(status.datesWithoutDoneBlob).isEmpty()
    assertThat(status.isStale).isNull()
    assertThat(status.staleDays).isNull()
  }

  @Test
  fun `checkGaps detects gap dates`(): Unit = runBlocking {
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

    val status = result.statuses.single()
    assertThat(status.gapDates)
      .containsExactly(LocalDate.of(2026, 3, 13), LocalDate.of(2026, 3, 14))
    assertThat(status.isStale).isNull()
  }

  @Test
  fun `checkGaps returns null fields when model line has no uploads`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkGaps()
    val status = result.statuses.single()
    assertThat(status.gapDates).isNull()
    assertThat(status.isStale).isNull()
  }

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

    val status = result.statuses.single()
    assertThat(status.gapDates).isEmpty()
    assertThat(status.zeroImpressionDates).containsExactly(LocalDate.of(2026, 3, 14))
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
    assertThat(result.statuses.single().zeroImpressionDates).isEmpty()
  }

  @Test
  fun `checkFullStatus skips non-date folder names`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    // Valid date folder
    ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-15")
    createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")
    createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")

    // Invalid date format folder - should be skipped
    val badPath = "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/not-a-date/done"
    File(tempFolder.root, badPath).parentFile.mkdirs()
    storageClient.writeBlob(badPath, ByteString.copyFromUtf8("done"))

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    val status = result.statuses.single()
    assertThat(status.latestDate).isEqualTo(LocalDate.of(2026, 3, 15))
    assertThat(status.gapDates).isEmpty()
  }

  @Test
  fun `checkGaps skips non-date folder names`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    // Valid date folder
    ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-15")
    createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")
    createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")

    // Invalid date format folder - should be skipped
    val badPath = "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/not-a-date/done"
    File(tempFolder.root, badPath).parentFile.mkdirs()
    storageClient.writeBlob(badPath, ByteString.copyFromUtf8("done"))

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkGaps()
    val status = result.statuses.single()
    assertThat(status.latestDate).isEqualTo(LocalDate.of(2026, 3, 15))
    assertThat(status.gapDates).isEmpty()
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

    val status = result.statuses.single()
    assertThat(status.gapDates).isEmpty()
    assertThat(status.zeroImpressionDates).containsExactly(LocalDate.of(2026, 3, 14))
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
  fun `constructor throws when edpImpressionPath starts with slash`() {
    val storageClient = createStorageClient()

    assertFailsWith<IllegalArgumentException> {
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = "/$EDP_IMPRESSION_PATH",
        activeModelLines = setOf(MODEL_LINE_A),
      )
    }
  }

  @Test
  fun `constructor throws when edpImpressionPath ends with slash`() {
    val storageClient = createStorageClient()

    assertFailsWith<IllegalArgumentException> {
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = "$EDP_IMPRESSION_PATH/",
        activeModelLines = setOf(MODEL_LINE_A),
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

    val status = result.statuses.single()
    assertThat(status.gapDates).isEmpty()
    assertThat(status.zeroImpressionDates).isEmpty()
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

    val status = result.statuses.single()
    assertThat(status.gapDates).isEmpty()
    assertThat(status.zeroImpressionDates).isEmpty()
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

    val status = result.statuses.single()
    assertThat(status.datesWithoutDoneBlob).containsExactly(LocalDate.of(2026, 3, 14))
    assertThat(status.gapDates).containsExactly(LocalDate.of(2026, 3, 14))
    assertThat(status.zeroImpressionDates).isEmpty()
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

    val status = result.statuses.single()
    assertThat(status.datesWithoutDoneBlob).containsExactly(LocalDate.of(2026, 3, 14))
    assertThat(status.gapDates).containsExactly(LocalDate.of(2026, 3, 14))
    assertThat(status.zeroImpressionDates).isEmpty()
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

    val status = result.statuses.single()
    // Stale: latest done date is March 12, today is March 15 = 3 days, not > 3
    // But gaps and other issues exist
    assertThat(status.staleDays).isEqualTo(3)
    assertThat(status.isStale).isFalse()
    assertThat(status.gapDates).containsExactly(LocalDate.of(2026, 3, 11))
    assertThat(status.zeroImpressionDates).containsExactly(LocalDate.of(2026, 3, 12))
    assertThat(status.datesWithoutDoneBlob).containsExactly(LocalDate.of(2026, 3, 13))
  }

  @Test
  fun `checkFullStatus emits stale days and gap metrics`(): Unit = runBlocking {
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

    monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })

    val metrics = collectMetrics()
    val metricByName = metrics.associateBy { it.name }

    assertThat(metricByName).containsKey(STALE_DAYS_METRIC)
    val stalePoint = metricByName.getValue(STALE_DAYS_METRIC).longGaugeData.points.single()
    assertThat(stalePoint.value).isEqualTo(5)
    assertThat(stalePoint.attributes.get(MODEL_LINE_ATTR)).isEqualTo(MODEL_LINE_A.toName())

    assertThat(getDateStatusCount(metrics, DataAvailabilityMonitorMetrics.STATUS_GAP)).isEqualTo(1)
  }

  @Test
  fun `checkFullStatus emits all metrics for combined issues`(): Unit = runBlocking {
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

    monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })

    val metrics = collectMetrics()
    val metricByName = metrics.associateBy { it.name }

    assertThat(metricByName).containsKey(STALE_DAYS_METRIC)
    assertThat(metricByName.getValue(STALE_DAYS_METRIC).longGaugeData.points.single().value)
      .isEqualTo(3)

    assertThat(getDateStatusCount(metrics, DataAvailabilityMonitorMetrics.STATUS_GAP)).isEqualTo(1)
    assertThat(getDateStatusCount(metrics, DataAvailabilityMonitorMetrics.STATUS_ZERO_IMPRESSION))
      .isEqualTo(1)
    assertThat(getDateStatusCount(metrics, DataAvailabilityMonitorMetrics.STATUS_WITHOUT_DONE_BLOB))
      .isEqualTo(1)
  }

  @Test
  fun `checkGaps emits gap metrics`(): Unit = runBlocking {
    val storageClient = createStorageClient()

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
      )

    monitor.checkGaps()

    val metrics = collectMetrics()
    val dateCountMetric = metrics.find { it.name == DATE_COUNT_METRIC }
    assertThat(dateCountMetric).isNotNull()
    val gapPoint =
      dateCountMetric!!.longSumData.points.find {
        it.attributes.get(DataAvailabilityMonitorMetrics.DATE_STATUS_ATTR) ==
          DataAvailabilityMonitorMetrics.STATUS_GAP
      }
    assertThat(gapPoint).isNotNull()
    assertThat(gapPoint!!.value).isEqualTo(2)
    assertThat(gapPoint.attributes.get(MODEL_LINE_ATTR)).isEqualTo(MODEL_LINE_A.toName())
    assertThat(gapPoint.attributes.get(EDP_IMPRESSION_PATH_ATTR)).isEqualTo(EDP_IMPRESSION_PATH)
  }

  @Test
  fun `checkFullStatus does not emit gap or issue metrics when healthy`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    for (day in 13..15) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })

    val metrics = collectMetrics()
    val metricByName = metrics.associateBy { it.name }

    assertThat(metricByName).containsKey(STALE_DAYS_METRIC)
    assertThat(metricByName.getValue(STALE_DAYS_METRIC).longGaugeData.points.single().value)
      .isEqualTo(0)

    assertThat(getDateStatusCount(metrics, DataAvailabilityMonitorMetrics.STATUS_HEALTHY))
      .isEqualTo(3)

    assertThat(getDateStatusCount(metrics, DataAvailabilityMonitorMetrics.STATUS_GAP)).isNull()
    assertThat(getDateStatusCount(metrics, DataAvailabilityMonitorMetrics.STATUS_ZERO_IMPRESSION))
      .isNull()
    assertThat(getDateStatusCount(metrics, DataAvailabilityMonitorMetrics.STATUS_WITHOUT_DONE_BLOB))
      .isNull()
    assertThat(getDateStatusCount(metrics, DataAvailabilityMonitorMetrics.STATUS_LATE_ARRIVING))
      .isNull()
  }

  @Test
  fun `checkFullStatus detects late-arriving data after done blob`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-15")
    createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")
    createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")

    val doneFile = getFile(MODEL_LINE_A.modelLineId, "2026-03-15", "done")
    val dataFile = getFile(MODEL_LINE_A.modelLineId, "2026-03-15", "data_campaign_1")
    dataFile.setLastModified(doneFile.lastModified() + 2000)

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })

    val status = result.statuses.single()
    assertThat(status.lateArrivingDates).containsExactly(LocalDate.of(2026, 3, 15))
  }

  @Test
  fun `checkFullStatus reports no late-arriving data when files arrive before done blob`(): Unit =
    runBlocking {
      val storageClient = createStorageClient()

      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-15")
      createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")

      val doneFile = getFile(MODEL_LINE_A.modelLineId, "2026-03-15", "done")
      val dataFile = getFile(MODEL_LINE_A.modelLineId, "2026-03-15", "data_campaign_1")
      doneFile.setLastModified(dataFile.lastModified() + 2000)

      val monitor =
        DataAvailabilityMonitor(
          storageClient = storageClient,
          edpImpressionPath = EDP_IMPRESSION_PATH,
          activeModelLines = setOf(MODEL_LINE_A),
        )

      val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })

      val status = result.statuses.single()
      assertThat(status.lateArrivingDates).isEmpty()
    }

  @Test
  fun `checkGaps detects late-arriving data after done blob`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-15")
    createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")
    createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")

    val doneFile = getFile(MODEL_LINE_A.modelLineId, "2026-03-15", "done")
    val dataFile = getFile(MODEL_LINE_A.modelLineId, "2026-03-15", "data_campaign_1")
    dataFile.setLastModified(doneFile.lastModified() + 2000)

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkGaps()

    val status = result.statuses.single()
    assertThat(status.lateArrivingDates).containsExactly(LocalDate.of(2026, 3, 15))
  }

  @Test
  fun `checkFullStatus emits late-arriving dates metric`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-15")
    createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")
    createDataFile(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")

    val doneFile = getFile(MODEL_LINE_A.modelLineId, "2026-03-15", "done")
    val dataFile = getFile(MODEL_LINE_A.modelLineId, "2026-03-15", "data_campaign_1")
    dataFile.setLastModified(doneFile.lastModified() + 2000)

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })

    val metrics = collectMetrics()
    assertThat(getDateStatusCount(metrics, DataAvailabilityMonitorMetrics.STATUS_LATE_ARRIVING))
      .isEqualTo(1)
  }

  // --- Spurious deletion check tests ---

  @Test
  fun `checkSpuriousDeletions throws when stub is not provided`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    assertFailsWith<IllegalArgumentException> {
      monitor.checkSpuriousDeletions(lookbackDays = 90, clock = { TODAY })
    }
  }

  @Test
  fun `checkSpuriousDeletions throws when lookbackDays is zero`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
        impressionMetadataStub = impressionMetadataStub,
        dataProviderName = DATA_PROVIDER_NAME,
      )

    assertFailsWith<IllegalArgumentException> {
      monitor.checkSpuriousDeletions(lookbackDays = 0, clock = { TODAY })
    }
  }

  @Test
  fun `checkSpuriousDeletions detects deleted entry whose blob still exists`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    // Create a blob on storage that matches the deleted entry's blobUri
    val blobPath = "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/2026-03-10/metadata_campaign_123.json"
    storageClient.writeBlob(blobPath, ByteString.copyFromUtf8("data"))

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
        impressionMetadataStub = impressionMetadataStub,
        dataProviderName = DATA_PROVIDER_NAME,
      )

    val result = monitor.checkSpuriousDeletions(lookbackDays = 90, clock = { TODAY })

    val status = result.statuses.single()
    assertThat(status.spuriousDeletionCount).isEqualTo(1)
    assertThat(status.legitimateDeletionCount).isEqualTo(0)
  }

  @Test
  fun `checkSpuriousDeletions counts legitimate deletion when blob is gone`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    // Do NOT create the blob - it's legitimately deleted

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
        impressionMetadataStub = impressionMetadataStub,
        dataProviderName = DATA_PROVIDER_NAME,
      )

    val result = monitor.checkSpuriousDeletions(lookbackDays = 90, clock = { TODAY })

    val status = result.statuses.single()
    assertThat(status.spuriousDeletionCount).isEqualTo(0)
    assertThat(status.legitimateDeletionCount).isEqualTo(1)
  }

  @Test
  fun `checkSpuriousDeletions returns zero counts when no deleted entries`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
        impressionMetadataStub = noDeletedEntriesStub,
        dataProviderName = DATA_PROVIDER_NAME,
      )

    val result = monitor.checkSpuriousDeletions(lookbackDays = 90, clock = { TODAY })

    val status = result.statuses.single()
    assertThat(status.spuriousDeletionCount).isEqualTo(0)
    assertThat(status.legitimateDeletionCount).isEqualTo(0)
  }

  @Test
  fun `checkSpuriousDeletions emits metrics`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    val blobPath = "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/2026-03-10/metadata_campaign_123.json"
    storageClient.writeBlob(blobPath, ByteString.copyFromUtf8("data"))

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
        impressionMetadataStub = impressionMetadataStub,
        dataProviderName = DATA_PROVIDER_NAME,
      )

    monitor.checkSpuriousDeletions(lookbackDays = 90, clock = { TODAY })

    val metrics = collectMetrics()
    assertThat(
        getDateStatusCount(metrics, DataAvailabilityMonitorMetrics.STATUS_SPURIOUS_DELETION)
      )
      .isEqualTo(1)
  }

}
