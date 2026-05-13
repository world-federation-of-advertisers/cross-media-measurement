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
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import com.google.protobuf.util.JsonFormat
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter
import java.io.File
import java.time.Clock
import java.time.Duration
import kotlin.test.assertFailsWith
import kotlin.test.fail
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponseKt.modelLineBoundMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.EntityKeyGroup
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.entityKey
import org.wfanet.measurement.edpaggregator.v1alpha.entityKeyGroup
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.storage.BlobMetadataStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

enum class BlobEncoding {
  PROTO,
  JSON,
  EMPTY,
}

@RunWith(JUnit4::class)
class DataAvailabilitySyncTest {

  private val bucket = "file:///my-bucket"
  private val folderPrefix = "edp/edpa_edp/timestamp/"
  private val dataProviderKeyAttributeKey =
    AttributeKey.stringKey("edpa.data_availability_sync.data_provider_key")
  private val syncStatusAttributeKey =
    AttributeKey.stringKey("edpa.data_availability_sync.sync_status")
  private val rpcMethodAttributeKey =
    AttributeKey.stringKey("edpa.data_availability_sync.rpc_method")
  private val statusCodeAttributeKey =
    AttributeKey.stringKey("edpa.data_availability_sync.status_code")

  companion object {
    private const val SYNC_DURATION_METRIC = "edpa.data_availability.sync_duration"
    private const val RECORDS_SYNCED_METRIC = "edpa.data_availability.records_synced"
    private const val CMMS_RPC_ERRORS_METRIC = "edpa.data_availability.cmms_rpc_errors"
    private const val DATE_COUNT_METRIC = "edpa.data_availability.date_count"
    private const val DEFAULT_BATCH_SIZE = 100
  }

  private val dataProvidersServiceMock: DataProvidersCoroutineImplBase = mockService {
    onBlocking { replaceDataAvailabilityIntervals(any<ReplaceDataAvailabilityIntervalsRequest>()) }
      .thenAnswer { DataProvider.getDefaultInstance() }
  }

  private val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListImpressionMetadataRequest>(0)

          // Build some fake proto data for the response
          listImpressionMetadataResponse {
            impressionMetadata +=
              listOf(
                impressionMetadata {
                  name = "${request.parent}/impressionMetadata1"
                  modelLine = request.filter.modelLine
                  blobUri = "gs://bucket/blob-1"
                  interval = interval {
                    startTime = timestamp { seconds = 100 }
                    endTime = timestamp { seconds = 200 }
                  }
                  state = ImpressionMetadata.State.ACTIVE
                },
                impressionMetadata {
                  name = "${request.parent}/impressionMetadata2"
                  modelLine = request.filter.modelLine
                  blobUri = "gs://bucket/blob-2"
                  interval = interval {
                    startTime = timestamp { seconds = 200 }
                    endTime = timestamp { seconds = 300 }
                  }
                  state = ImpressionMetadata.State.ACTIVE
                },
              )
          }
        }
      onBlocking { batchCreateImpressionMetadata(any<BatchCreateImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<BatchCreateImpressionMetadataRequest>(0)
          batchCreateImpressionMetadataResponse {
            impressionMetadata +=
              request.requestsList.mapIndexed { index, createRequest ->
                // Return the input metadata with a name set (simulating server response)
                createRequest.impressionMetadata
                  .toBuilder()
                  .setName("${request.parent}/impressionMetadata/im-$index")
                  .build()
              }
          }
        }
      onBlocking { computeModelLineBounds(any<ComputeModelLineBoundsRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ComputeModelLineBoundsRequest>(0)
          // Build some fake proto data for the response
          computeModelLineBoundsResponse {
            modelLineBounds += modelLineBoundMapEntry {
              key = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLineA"
              value = interval {
                startTime = timestamp { seconds = 100 }
                endTime = timestamp { seconds = 200 }
              }
            }
          }
        }
    }

  private val dataProvidersStub: DataProvidersCoroutineStub by lazy {
    DataProvidersCoroutineStub(grpcTestServerRule.channel)
  }

  private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub by lazy {
    ImpressionMetadataServiceCoroutineStub(grpcTestServerRule.channel)
  }

  private data class MetricsTestEnvironment(
    val metrics: DataAvailabilitySyncMetrics,
    val metricExporter: InMemoryMetricExporter,
    val metricReader: PeriodicMetricReader,
    val openTelemetry: OpenTelemetrySdk,
  ) {
    fun close() {
      openTelemetry.close()
      GlobalOpenTelemetry.resetForTest()
      Instrumentation.resetForTest()
    }
  }

  private fun createMetricsEnvironment(): MetricsTestEnvironment {
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    val metricExporter = InMemoryMetricExporter.create()
    val metricReader = PeriodicMetricReader.create(metricExporter)
    val meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build()
    val openTelemetry =
      OpenTelemetrySdk.builder().setMeterProvider(meterProvider).buildAndRegisterGlobal()
    val meter = meterProvider.get("data-availability-sync-test")
    return MetricsTestEnvironment(
      DataAvailabilitySyncMetrics(meter),
      metricExporter,
      metricReader,
      openTelemetry,
    )
  }

  private fun getDateStatusCount(metrics: List<MetricData>, status: String): Long? {
    val dateCountMetric = metrics.find { it.name == DATE_COUNT_METRIC } ?: return null
    return dateCountMetric.longSumData.points
      .find { it.attributes.get(DataAvailabilityMonitorMetrics.DATE_STATUS_ATTR) == status }
      ?.value
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(dataProvidersServiceMock)
    addService(impressionMetadataServiceMock)
  }

  @get:Rule val tempFolder = TemporaryFolder()

  @Test
  fun `register single contiguous day for existing model line using proto message`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    val batchCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchCreateImpressionMetadata(batchCaptor.capture())
    }
    assertThat(batchCaptor.firstValue.requestsCount).isEqualTo(1)
    val boundsRequestCaptor = argumentCaptor<ComputeModelLineBoundsRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      computeModelLineBounds(boundsRequestCaptor.capture())
    }
    assertThat(boundsRequestCaptor.firstValue.parent).isEqualTo("dataProviders/dataProvider123")
  }

  @Test
  fun `register single contiguous day for existing model line using json message`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L), BlobEncoding.JSON)

    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    val batchCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchCreateImpressionMetadata(batchCaptor.capture())
    }
    assertThat(batchCaptor.firstValue.requestsCount).isEqualTo(1)
    verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }
  }

  @Test
  fun `sync updates availability for mapped model lines`() {
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

      val modelLineKey = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLineA"
      val mappedLines =
        listOf(
          "modelProviders/provider1/modelSuites/suite1/modelLines/modelLineB",
          "dataProviders/dataProvider123/modelLines/modelLineC",
        )

      val dataAvailabilitySync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = mapOf(modelLineKey to mappedLines),
          errorIfGapsExist = true,
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

      val requestCaptor = argumentCaptor<ReplaceDataAvailabilityIntervalsRequest>()
      verifyBlocking(dataProvidersServiceMock, times(1)) {
        replaceDataAvailabilityIntervals(requestCaptor.capture())
      }
      val availabilityKeys = requestCaptor.firstValue.dataAvailabilityIntervalsList.map { it.key }
      assertThat(availabilityKeys).containsExactlyElementsIn(mappedLines)
    }
  }

  @Test
  fun `sync updates availability for existing and new model lines`() {
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      val existingModelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLineA"
      val newModelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLineB"

      seedBlobDetailsWithModelLine(storageClient, folderPrefix, listOf(300L to 400L), newModelLine)

      wheneverBlocking { impressionMetadataServiceMock.computeModelLineBounds(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ComputeModelLineBoundsRequest>(0)
          computeModelLineBoundsResponse {
            modelLineBounds += modelLineBoundMapEntry {
              key = existingModelLine
              value = interval {
                startTime = timestamp { seconds = 100 }
                endTime = timestamp { seconds = 200 }
              }
            }
            modelLineBounds += modelLineBoundMapEntry {
              key = newModelLine
              value = interval {
                startTime = timestamp { seconds = 300 }
                endTime = timestamp { seconds = 400 }
              }
            }
          }
        }

      val dataAvailabilitySync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

      val requestCaptor = argumentCaptor<ReplaceDataAvailabilityIntervalsRequest>()
      verifyBlocking(dataProvidersServiceMock, times(1)) {
        replaceDataAvailabilityIntervals(requestCaptor.capture())
      }
      val availabilityKeys = requestCaptor.firstValue.dataAvailabilityIntervalsList.map { it.key }
      assertThat(availabilityKeys).containsExactly(existingModelLine, newModelLine)
    }
  }

  @Test
  fun `register single overlapping day for existing model line`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(storageClient, folderPrefix, listOf(250L to 400L), BlobEncoding.JSON)

    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    val batchCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchCreateImpressionMetadata(batchCaptor.capture())
    }
    assertThat(batchCaptor.firstValue.requestsCount).isEqualTo(1)
    verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }
  }

  @Test
  fun `registers a single contiguous day preceding an existing interval for an existing model line`() =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      seedBlobDetails(storageClient, folderPrefix, listOf(50L to 100L), BlobEncoding.JSON)

      val dataAvailabilitySync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
      verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchCreateImpressionMetadata(any())
      }
      verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }
    }

  @Test
  fun `register multiple contiguous day for existing model line`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(
      storageClient,
      folderPrefix,
      listOf(300L to 400L, 400L to 500L, 500L to 600L),
      BlobEncoding.JSON,
    )

    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(1)) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }
  }

  @Test
  fun `blob details with missing interval throws IllegalArgumentException`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(
      storageClient,
      folderPrefix,
      listOf(300L to 400L, null to null),
      BlobEncoding.JSON,
    )

    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    try {
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
      fail("Expected IllegalArgumentException")
    } catch (e: IllegalArgumentException) {
      // Expected.
    }
  }

  @Test
  fun `blob details with wrong file extension throws IllegalArgumentException`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L), BlobEncoding.EMPTY)

    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    try {
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
      fail("Expected IllegalArgumentException")
    } catch (e: IllegalArgumentException) {
      // Expected.
    }
  }

  @Test
  fun `sync throw if file prefix doesn't follow expected path`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    try {
      dataAvailabilitySync.sync("$bucket/some-wrong-path/done")
      fail("Expected IllegalArgumentException")
    } catch (e: IllegalArgumentException) {
      // Expected.
    }
  }

  @Test
  fun `metadata file is ignored if no associated impression file is found`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L), createImpressionFile = false)

    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(0)) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(0)) { computeModelLineBounds(any()) }
  }

  @Test
  fun `sync emits success metrics`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

    val metricsEnv = createMetricsEnvironment()
    try {
      val dataAvailabilitySync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
          metrics = metricsEnv.metrics,
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      val durationPoint = metricByName.getValue(SYNC_DURATION_METRIC).histogramData.points.single()
      assertThat(durationPoint.attributes.get(syncStatusAttributeKey)).isEqualTo("success")
      assertThat(durationPoint.attributes.get(dataProviderKeyAttributeKey))
        .isEqualTo("dataProviders/dataProvider123")

      val recordsPoint = metricByName.getValue(RECORDS_SYNCED_METRIC).longSumData.points.single()
      assertThat(recordsPoint.value).isEqualTo(1)
      assertThat(recordsPoint.attributes.get(syncStatusAttributeKey)).isEqualTo("success")
      assertThat(recordsPoint.attributes.get(dataProviderKeyAttributeKey))
        .isEqualTo("dataProviders/dataProvider123")
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `sync emits failure metrics when replaceDataAvailabilityIntervals fails`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

    val metricsEnv = createMetricsEnvironment()
    try {
      val dataAvailabilitySync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
          metrics = metricsEnv.metrics,
        )

      wheneverBlocking { dataProvidersServiceMock.replaceDataAvailabilityIntervals(any()) }
        .thenAnswer { throw StatusException(Status.UNAVAILABLE) }

      try {
        dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
        fail("Expected Exception")
      } catch (e: Exception) {
        // Expected.
      } finally {
        wheneverBlocking { dataProvidersServiceMock.replaceDataAvailabilityIntervals(any()) }
          .thenAnswer { DataProvider.getDefaultInstance() }
      }

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      val durationPoint = metricByName.getValue(SYNC_DURATION_METRIC).histogramData.points.single()
      assertThat(durationPoint.attributes.get(syncStatusAttributeKey)).isEqualTo("failed")
      assertThat(durationPoint.attributes.get(dataProviderKeyAttributeKey))
        .isEqualTo("dataProviders/dataProvider123")

      assertThat(metricByName.containsKey(RECORDS_SYNCED_METRIC)).isFalse()

      val cmmsErrorPoint = metricByName.getValue(CMMS_RPC_ERRORS_METRIC).longSumData.points.single()
      assertThat(cmmsErrorPoint.value).isEqualTo(1)
      assertThat(cmmsErrorPoint.attributes.get(dataProviderKeyAttributeKey))
        .isEqualTo("dataProviders/dataProvider123")
      assertThat(cmmsErrorPoint.attributes.get(rpcMethodAttributeKey))
        .isEqualTo("ReplaceDataAvailabilityIntervals")
      assertThat(cmmsErrorPoint.attributes.get(statusCodeAttributeKey))
        .isEqualTo(Status.Code.UNAVAILABLE.name)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `saveImpressionMetadata generates deterministic UUIDs`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
    dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

    // Capture both requests
    val captor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(2)) {
      batchCreateImpressionMetadata(captor.capture())
    }

    val requestIds =
      captor.allValues.flatMap { request -> request.requestsList.map { it.requestId } }
    assertThat(requestIds.distinct().size).isEqualTo(1)
  }

  @Test
  fun `saveImpressionMetadata batches according to batch size and throttles each batch`() =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      seedBlobDetails(
        storageClient,
        folderPrefix,
        listOf(100L to 200L, 200L to 300L, 300L to 400L),
        BlobEncoding.JSON,
      )

      val recordingThrottler = RecordingThrottler()
      val dataAvailabilitySync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          recordingThrottler,
          impressionMetadataBatchSize = 2,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

      val captor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(2)) {
        batchCreateImpressionMetadata(captor.capture())
      }
      assertThat(captor.allValues.map { it.requestsCount }).containsExactly(2, 1).inOrder()
      // Two batches plus one call when updating availability intervals.
      assertThat(recordingThrottler.onReadyCalls).isEqualTo(3)
    }

  @Test
  fun `invalid path throws exception`() {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)
    runBlocking {
      seedBlobDetails(
        storageClient,
        folderPrefix,
        listOf(100L to 200L, 200L to 300L, 300L to 400L),
        BlobEncoding.JSON,
      )
    }

    val recordingThrottler = RecordingThrottler()
    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/some-other-edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        recordingThrottler,
        impressionMetadataBatchSize = 2,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    assertFailsWith<IllegalArgumentException> {
      runBlocking { dataAvailabilitySync.sync("$bucket/${folderPrefix}done") }
    }
  }

  @Test
  fun `supports different subfolders`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(
      storageClient,
      "edp/edpa_edp/model-line/some-model-line/timestamp/",
      listOf(300L to 400L, 400L to 500L, 500L to 600L),
      BlobEncoding.JSON,
    )

    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    dataAvailabilitySync.sync("$bucket/edp/edpa_edp/model-line/some-model-line/timestamp/done")
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    val impressionMetadataRequests =
      listOf(300L to 400L, 400L to 500L, 500L to 600L).mapIndexed { index, times ->
        impressionMetadata {
          blobUri =
            "file:///my-bucket/edp/edpa_edp/model-line/some-model-line/timestamp/metadata-$index.json"
          blobTypeUrl =
            "type.googleapis.com/wfa.measurement.securecomputation.impressions.BlobDetails"
          eventGroupReferenceId = "some-event-group-reference-id"
          modelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLine1"
          this.interval = interval {
            startTime = timestamp { seconds = times.first }
            endTime = timestamp { seconds = times.second }
          }
        }
      }
    val batchCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchCreateImpressionMetadata(batchCaptor.capture())
    }
    val createdItems = batchCaptor.firstValue.requestsList.map { it.impressionMetadata }
    // File system traversal order is non-deterministic, so use unordered comparison
    assertThat(createdItems).containsExactlyElementsIn(impressionMetadataRequests)
    verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }
  }

  @Test
  fun `updateBlobMetadata is called for both metadata and impressions files`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

    // Should have exactly 2 calls: one for metadata file, one for impressions file
    assertThat(storageClient.updateBlobMetadataCalls).hasSize(2)

    // Verify metadata file update (has resource ID in metadata)
    val metadataFileUpdate =
      storageClient.updateBlobMetadataCalls.single { it.metadata.isNotEmpty() }
    assertThat(metadataFileUpdate.blobKey).contains("metadata")
    assertThat(metadataFileUpdate.customCreateTime).isNotNull()
    assertThat(metadataFileUpdate.metadata)
      .containsKey(DataAvailabilitySync.IMPRESSION_METADATA_RESOURCE_ID_KEY)

    // Verify impressions file update (no metadata, just customCreateTime)
    val impressionsFileUpdate =
      storageClient.updateBlobMetadataCalls.single { it.metadata.isEmpty() }
    assertThat(impressionsFileUpdate.blobKey).contains("some_blob_uri")
    assertThat(impressionsFileUpdate.customCreateTime).isNotNull()

    // Both files should have the same customCreateTime (derived from interval start time)
    assertThat(metadataFileUpdate.customCreateTime)
      .isEqualTo(impressionsFileUpdate.customCreateTime)
  }

  @Test
  fun `updateBlobMetadata uses impressions blob key from BlobDetails not inferred from metadata URI`() =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      // Create a BlobDetails where blob_uri points to a path that is NOT derived from the metadata
      // file name. The old implementation would have tried to infer the impressions blob key by
      // string replacement on the metadata URI.
      val customImpressionsPath = "custom/path/to/impressions_data"
      seedBlobDetailsWithCustomImpressionsPath(
        storageClient = storageClient,
        prefix = folderPrefix,
        intervals = listOf(300L to 400L),
        impressionsBlobKey = customImpressionsPath,
      )

      val dataAvailabilitySync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

      // Verify that updateBlobMetadata was called with the correct impressions blob key
      // from BlobDetails, not inferred from the metadata URI
      val impressionsUpdateCalls =
        storageClient.updateBlobMetadataCalls.filter { it.blobKey == customImpressionsPath }
      assertThat(impressionsUpdateCalls).hasSize(1)
      // Impressions blob should have customCreateTime set but no metadata (resource ID)
      assertThat(impressionsUpdateCalls.first().customCreateTime).isNotNull()
      assertThat(impressionsUpdateCalls.first().metadata).isEmpty()

      // Verify the blob details (metadata) file update was called with resource ID
      val metadataUpdateCalls =
        storageClient.updateBlobMetadataCalls.filter {
          it.blobKey.contains("metadata") && it.metadata.isNotEmpty()
        }
      assertThat(metadataUpdateCalls).hasSize(1)
      // Blob details should have customCreateTime set and contain the resource ID
      val metadataUpdate = metadataUpdateCalls.first()
      assertThat(metadataUpdate.customCreateTime).isNotNull()
      assertThat(metadataUpdate.metadata)
        .containsKey(DataAvailabilitySync.IMPRESSION_METADATA_RESOURCE_ID_KEY)
      assertThat(metadataUpdate.metadata[DataAvailabilitySync.IMPRESSION_METADATA_RESOURCE_ID_KEY])
        .isEqualTo("dataProviders/dataProvider123/impressionMetadata/im-0")

      // Verify both files have the same customCreateTime (derived from the interval start time)
      assertThat(impressionsUpdateCalls.first().customCreateTime)
        .isEqualTo(metadataUpdateCalls.first().customCreateTime)
    }

  @Test
  fun `works with blank edp impression path`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(
      storageClient,
      "edp/edpa_edp/model-line/some-model-line/timestamp/",
      listOf(300L to 400L, 400L to 500L, 500L to 600L),
      BlobEncoding.JSON,
      edpImpressionPath = "",
    )

    val dataAvailabilitySync =
      DataAvailabilitySync(
        "",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    dataAvailabilitySync.sync("$bucket/edp/edpa_edp/model-line/some-model-line/timestamp/done")
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    val impressionMetadataRequests =
      listOf(300L to 400L, 400L to 500L, 500L to 600L).mapIndexed { index, times ->
        impressionMetadata {
          blobUri =
            "file:///my-bucket/edp/edpa_edp/model-line/some-model-line/timestamp/metadata-$index.json"
          blobTypeUrl =
            "type.googleapis.com/wfa.measurement.securecomputation.impressions.BlobDetails"
          eventGroupReferenceId = "some-event-group-reference-id"
          modelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLine1"
          this.interval = interval {
            startTime = timestamp { seconds = times.first }
            endTime = timestamp { seconds = times.second }
          }
        }
      }
    val batchCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchCreateImpressionMetadata(batchCaptor.capture())
    }
    val createdItems = batchCaptor.firstValue.requestsList.map { it.impressionMetadata }
    // File system traversal order is non-deterministic, so use unordered comparison
    assertThat(createdItems).containsExactlyElementsIn(impressionMetadataRequests)
    verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }
  }

  @Test
  fun `sync skips replaceDataAvailabilityIntervals when date gaps are detected`(): Unit =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      // Set up done blobs at model-line paths with a gap (missing 2026-03-14)
      val modelLineId = "modelLine1"
      val edpPath = "edp/edpa_edp"
      for (date in listOf("2026-03-13", "2026-03-15")) {
        val dataPath = "$edpPath/model-line/$modelLineId/$date/data_campaign_1"
        File(tempFolder.root, dataPath).parentFile.mkdirs()
        storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
        val donePath = "$edpPath/model-line/$modelLineId/$date/done"
        storageClient.writeBlob(donePath, ByteString.copyFromUtf8("done"))
      }

      seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

      val dataAvailabilitySync =
        DataAvailabilitySync(
          edpPath,
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

      // Impression metadata should still be saved
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchCreateImpressionMetadata(any())
      }

      // Model line bounds should still be computed
      verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }

      // replaceDataAvailabilityIntervals should NOT be called due to gaps
      verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
    }

  @Test
  fun `sync calls replaceDataAvailabilityIntervals when no date gaps exist`(): Unit = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    // Set up done blobs at model-line paths with no gaps
    val modelLineId = "modelLine1"
    val edpPath = "edp/edpa_edp"
    for (date in listOf("2026-03-13", "2026-03-14", "2026-03-15")) {
      val dataPath = "$edpPath/model-line/$modelLineId/$date/data_campaign_1"
      File(tempFolder.root, dataPath).parentFile.mkdirs()
      storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
      val donePath = "$edpPath/model-line/$modelLineId/$date/done"
      storageClient.writeBlob(donePath, ByteString.copyFromUtf8("done"))
    }

    // Seed metadata in the sync trigger folder
    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

    val dataAvailabilitySync =
      DataAvailabilitySync(
        edpPath,
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

    // replaceDataAvailabilityIntervals should be called since no gaps
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
  }

  @Test
  fun `sync emits monitor gap metrics when date gaps are detected`(): Unit = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    val modelLineId = "modelLine1"
    val edpPath = "edp/edpa_edp"
    // Set up done blobs with a gap (missing 2026-03-14)
    for (date in listOf("2026-03-13", "2026-03-15")) {
      val dataPath = "$edpPath/model-line/$modelLineId/$date/data_campaign_1"
      File(tempFolder.root, dataPath).parentFile.mkdirs()
      storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
      val donePath = "$edpPath/model-line/$modelLineId/$date/done"
      storageClient.writeBlob(donePath, ByteString.copyFromUtf8("done"))
    }

    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

    val metricsEnv = createMetricsEnvironment()
    try {
      val dataAvailabilitySync =
        DataAvailabilitySync(
          edpPath,
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
          metrics = metricsEnv.metrics,
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems

      assertThat(getDateStatusCount(metricData, DataAvailabilityMonitorMetrics.STATUS_GAP))
        .isEqualTo(1)
      assertThat(
          getDateStatusCount(metricData, DataAvailabilityMonitorMetrics.STATUS_ZERO_IMPRESSION)
        )
        .isNull()
      assertThat(
          getDateStatusCount(metricData, DataAvailabilityMonitorMetrics.STATUS_WITHOUT_DONE_BLOB)
        )
        .isNull()
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `sync does not emit gap metrics when no issues exist`(): Unit = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    val modelLineId = "modelLine1"
    val edpPath = "edp/edpa_edp"
    for (date in listOf("2026-03-13", "2026-03-14", "2026-03-15")) {
      val dataPath = "$edpPath/model-line/$modelLineId/$date/data_campaign_1"
      File(tempFolder.root, dataPath).parentFile.mkdirs()
      storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
      val donePath = "$edpPath/model-line/$modelLineId/$date/done"
      storageClient.writeBlob(donePath, ByteString.copyFromUtf8("done"))
    }

    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

    val metricsEnv = createMetricsEnvironment()
    try {
      val dataAvailabilitySync =
        DataAvailabilitySync(
          edpPath,
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
          metrics = metricsEnv.metrics,
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems

      assertThat(getDateStatusCount(metricData, DataAvailabilityMonitorMetrics.STATUS_HEALTHY))
        .isEqualTo(3)
      assertThat(getDateStatusCount(metricData, DataAvailabilityMonitorMetrics.STATUS_GAP)).isNull()
      assertThat(
          getDateStatusCount(metricData, DataAvailabilityMonitorMetrics.STATUS_ZERO_IMPRESSION)
        )
        .isNull()
      assertThat(
          getDateStatusCount(metricData, DataAvailabilityMonitorMetrics.STATUS_WITHOUT_DONE_BLOB)
        )
        .isNull()
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `sync does not throw when errorIfGapsExist is false and gaps exist`(): Unit = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    // Set up done blobs at model-line paths with a gap (missing 2026-03-14)
    val modelLineId = "modelLine1"
    val edpPath = "edp/edpa_edp"
    for (date in listOf("2026-03-13", "2026-03-15")) {
      val dataPath = "$edpPath/model-line/$modelLineId/$date/data_campaign_1"
      File(tempFolder.root, dataPath).parentFile.mkdirs()
      storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
      val donePath = "$edpPath/model-line/$modelLineId/$date/done"
      storageClient.writeBlob(donePath, ByteString.copyFromUtf8("done"))
    }

    // Seed metadata in the sync trigger folder
    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

    val dataAvailabilitySync =
      DataAvailabilitySync(
        edpPath,
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = false,
      )

    // Should NOT throw despite gaps
    dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

    // Impression metadata should still be saved
    verifyBlocking(impressionMetadataServiceMock, times(1)) { batchCreateImpressionMetadata(any()) }

    // Model line bounds should still be computed
    verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }

    // replaceDataAvailabilityIntervals should still be called despite gaps
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
  }

  @Test
  fun `sync calls replaceDataAvailabilityIntervals when errorIfGapsExist is false and no gaps exist`():
    Unit = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    // Set up done blobs at model-line paths with no gaps
    val modelLineId = "modelLine1"
    val edpPath = "edp/edpa_edp"
    for (date in listOf("2026-03-13", "2026-03-14", "2026-03-15")) {
      val dataPath = "$edpPath/model-line/$modelLineId/$date/data_campaign_1"
      File(tempFolder.root, dataPath).parentFile.mkdirs()
      storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
      val donePath = "$edpPath/model-line/$modelLineId/$date/done"
      storageClient.writeBlob(donePath, ByteString.copyFromUtf8("done"))
    }

    // Seed metadata in the sync trigger folder
    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

    val dataAvailabilitySync =
      DataAvailabilitySync(
        edpPath,
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = false,
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

    // replaceDataAvailabilityIntervals should be called since no gaps
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
  }

  @Test
  fun `sync emits success metrics when errorIfGapsExist is false and gaps exist`(): Unit =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      // Set up done blobs with a gap (missing 2026-03-14)
      val modelLineId = "modelLine1"
      val edpPath = "edp/edpa_edp"
      for (date in listOf("2026-03-13", "2026-03-15")) {
        val dataPath = "$edpPath/model-line/$modelLineId/$date/data_campaign_1"
        File(tempFolder.root, dataPath).parentFile.mkdirs()
        storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
        val donePath = "$edpPath/model-line/$modelLineId/$date/done"
        storageClient.writeBlob(donePath, ByteString.copyFromUtf8("done"))
      }

      seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

      val metricsEnv = createMetricsEnvironment()
      try {
        val dataAvailabilitySync =
          DataAvailabilitySync(
            edpPath,
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
            impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
            modelLineMap = emptyMap(),
            errorIfGapsExist = false,
            metrics = metricsEnv.metrics,
          )

        dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

        metricsEnv.metricReader.forceFlush()
        val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems

        // Should record success (not failure) since errorIfGapsExist=false prevents the throw
        val durationPoint =
          metricData
            .associateBy { it.name }
            .getValue(SYNC_DURATION_METRIC)
            .histogramData
            .points
            .single()
        assertThat(durationPoint.attributes.get(syncStatusAttributeKey)).isEqualTo("success")

        // Gap metrics should still be emitted
        assertThat(getDateStatusCount(metricData, DataAvailabilityMonitorMetrics.STATUS_GAP))
          .isEqualTo(1)
      } finally {
        metricsEnv.close()
      }
    }

  @Test
  fun `BlobDetails with only event_group_reference_id propagates ref id and empty entity_keys`() =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      // seedBlobDetails populates event_group_reference_id by default and no entity_keys.
      seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

      val dataAvailabilitySync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

      val batchCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchCreateImpressionMetadata(batchCaptor.capture())
      }
      val createdMetadata = batchCaptor.firstValue.requestsList.single().impressionMetadata
      assertThat(createdMetadata.eventGroupReferenceId).isEqualTo("some-event-group-reference-id")
      assertThat(createdMetadata.entityKeysList).isEmpty()
      // Data availability is updated regardless of which selectors BlobDetails carries.
      verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    }

  @Test
  fun `BlobDetails with only entity_keys flattens entity_keys and leaves ref id empty`() =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      val entityKeyGroups =
        listOf(
          entityKeyGroup {
            entityType = "campaign"
            entityIds += "campaign-1"
            entityIds += "campaign-2"
          },
          entityKeyGroup {
            entityType = "ad"
            entityIds += "ad-1"
          },
        )

      seedBlobDetails(
        storageClient,
        folderPrefix,
        listOf(300L to 400L),
        entityKeyGroups = entityKeyGroups,
        populateEventGroupReferenceId = false,
      )

      val dataAvailabilitySync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

      val batchCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchCreateImpressionMetadata(batchCaptor.capture())
      }
      val createdMetadata = batchCaptor.firstValue.requestsList.single().impressionMetadata
      assertThat(createdMetadata.eventGroupReferenceId).isEmpty()
      assertThat(createdMetadata.entityKeysList)
        .containsExactly(
          entityKey {
            entityType = "campaign"
            entityId = "campaign-1"
          },
          entityKey {
            entityType = "campaign"
            entityId = "campaign-2"
          },
          entityKey {
            entityType = "ad"
            entityId = "ad-1"
          },
        )
        .inOrder()
      // Data availability is updated regardless of which selectors BlobDetails carries.
      verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    }

  @Test
  fun `sync throws when BlobDetails has neither event_group_reference_id nor entity_keys`() =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      seedBlobDetails(
        storageClient,
        folderPrefix,
        listOf(300L to 400L),
        populateEventGroupReferenceId = false,
      )

      val dataAvailabilitySync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
        )

      assertFailsWith<IllegalArgumentException> {
        dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
      }
      // Neither metadata persistence nor data availability should happen on validation failure.
      verifyBlocking(impressionMetadataServiceMock, times(0)) {
        batchCreateImpressionMetadata(any())
      }
      verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
    }

  @Test
  fun `sync throws when EntityKeyGroup has empty entity_type`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(
      storageClient,
      folderPrefix,
      listOf(300L to 400L),
      entityKeyGroups =
        listOf(
          entityKeyGroup {
            // entity_type intentionally left empty.
            entityIds += "campaign-1"
          }
        ),
    )

    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    assertFailsWith<IllegalArgumentException> {
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
    }
    verifyBlocking(impressionMetadataServiceMock, times(0)) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
  }

  @Test
  fun `sync throws when EntityKeyGroup has empty entity_ids`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(
      storageClient,
      folderPrefix,
      listOf(300L to 400L),
      entityKeyGroups =
        listOf(
          entityKeyGroup {
            entityType = "campaign"
            // entity_ids intentionally left empty.
          }
        ),
    )

    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    assertFailsWith<IllegalArgumentException> {
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
    }
    verifyBlocking(impressionMetadataServiceMock, times(0)) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
  }

  @Test
  fun `sync throws when EntityKeyGroup has an empty entity_id`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    seedBlobDetails(
      storageClient,
      folderPrefix,
      listOf(300L to 400L),
      entityKeyGroups =
        listOf(
          entityKeyGroup {
            entityType = "campaign"
            entityIds += "campaign-1"
            // Second id intentionally empty.
            entityIds += ""
          }
        ),
    )

    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )

    assertFailsWith<IllegalArgumentException> {
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
    }
    verifyBlocking(impressionMetadataServiceMock, times(0)) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
  }

  @Test
  fun `BlobDetails with both event_group_reference_id and entity_keys propagates both`() =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      val entityKeyGroups =
        listOf(
          entityKeyGroup {
            entityType = "campaign"
            entityIds += "campaign-1"
            entityIds += "campaign-2"
          },
          entityKeyGroup {
            entityType = "ad"
            entityIds += "ad-1"
          },
        )

      seedBlobDetails(
        storageClient,
        folderPrefix,
        listOf(300L to 400L),
        entityKeyGroups = entityKeyGroups,
      )

      val dataAvailabilitySync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

      val batchCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchCreateImpressionMetadata(batchCaptor.capture())
      }
      val createdMetadata = batchCaptor.firstValue.requestsList.single().impressionMetadata
      assertThat(createdMetadata.eventGroupReferenceId).isEqualTo("some-event-group-reference-id")
      assertThat(createdMetadata.entityKeysList)
        .containsExactly(
          entityKey {
            entityType = "campaign"
            entityId = "campaign-1"
          },
          entityKey {
            entityType = "campaign"
            entityId = "campaign-2"
          },
          entityKey {
            entityType = "ad"
            entityId = "ad-1"
          },
        )
        .inOrder()
      // Data availability is updated regardless of which selectors BlobDetails carries.
      verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    }

  @Test
  fun `sync parses JSON-encoded BlobDetails with event_group_reference_id and entity_keys and propagates both`() =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      val entityKeyGroups =
        listOf(
          entityKeyGroup {
            entityType = "campaign"
            entityIds += "campaign-1"
            entityIds += "campaign-2"
          },
          entityKeyGroup {
            entityType = "ad"
            entityIds += "ad-1"
          },
        )

      seedBlobDetails(
        storageClient,
        folderPrefix,
        listOf(300L to 400L),
        encoding = BlobEncoding.JSON,
        entityKeyGroups = entityKeyGroups,
      )

      val dataAvailabilitySync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

      val batchCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchCreateImpressionMetadata(batchCaptor.capture())
      }
      val createdMetadata = batchCaptor.firstValue.requestsList.single().impressionMetadata
      assertThat(createdMetadata.eventGroupReferenceId).isEqualTo("some-event-group-reference-id")
      assertThat(createdMetadata.entityKeysList)
        .containsExactly(
          entityKey {
            entityType = "campaign"
            entityId = "campaign-1"
          },
          entityKey {
            entityType = "campaign"
            entityId = "campaign-2"
          },
          entityKey {
            entityType = "ad"
            entityId = "ad-1"
          },
        )
        .inOrder()
      verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    }

  @Test
  fun `sync parses JSON-encoded BlobDetails without event_group_reference_id and propagates only entity_keys`() =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      val entityKeyGroups =
        listOf(
          entityKeyGroup {
            entityType = "campaign"
            entityIds += "campaign-1"
            entityIds += "campaign-2"
          },
          entityKeyGroup {
            entityType = "ad"
            entityIds += "ad-1"
          },
        )

      seedBlobDetails(
        storageClient,
        folderPrefix,
        listOf(300L to 400L),
        encoding = BlobEncoding.JSON,
        entityKeyGroups = entityKeyGroups,
        populateEventGroupReferenceId = false,
      )

      val dataAvailabilitySync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

      val batchCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchCreateImpressionMetadata(batchCaptor.capture())
      }
      val createdMetadata = batchCaptor.firstValue.requestsList.single().impressionMetadata
      assertThat(createdMetadata.eventGroupReferenceId).isEmpty()
      assertThat(createdMetadata.entityKeysList)
        .containsExactly(
          entityKey {
            entityType = "campaign"
            entityId = "campaign-1"
          },
          entityKey {
            entityType = "campaign"
            entityId = "campaign-2"
          },
          entityKey {
            entityType = "ad"
            entityId = "ad-1"
          },
        )
        .inOrder()
      verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    }

  @Test
  fun `sync parses JSON-encoded BlobDetails with only event_group_reference_id and propagates ref id and empty entity_keys`() =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      // No entity_keys; legacy-style blob with only event_group_reference_id.
      seedBlobDetails(
        storageClient,
        folderPrefix,
        listOf(300L to 400L),
        encoding = BlobEncoding.JSON,
      )

      val dataAvailabilitySync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done")

      val batchCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchCreateImpressionMetadata(batchCaptor.capture())
      }
      val createdMetadata = batchCaptor.firstValue.requestsList.single().impressionMetadata
      assertThat(createdMetadata.eventGroupReferenceId).isEqualTo("some-event-group-reference-id")
      assertThat(createdMetadata.entityKeysList).isEmpty()
      verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    }

  /**
   * Seeds a directory (prefix) with BlobDetails files, one per interval. Returns the blob keys
   * written.
   */
  suspend fun seedBlobDetails(
    storageClient: StorageClient,
    prefix: String,
    intervals: List<Pair<Long?, Long?>>,
    encoding: BlobEncoding = BlobEncoding.PROTO,
    createImpressionFile: Boolean = true,
    edpImpressionPath: String = "edp/edpa_edp",
    entityKeyGroups: List<EntityKeyGroup> = emptyList(),
    populateEventGroupReferenceId: Boolean = true,
  ): List<String> {
    require(prefix.isEmpty() || prefix.endsWith("/")) { "prefix should end with '/'" }

    val written = mutableListOf<String>()

    intervals.forEachIndexed { index, (startSeconds, endSeconds) ->
      val blobUri = "$bucket/${folderPrefix}some_blob_uri_$index"
      val objectKey = "${folderPrefix}some_blob_uri_$index"
      val details = blobDetails {
        this.blobUri = blobUri
        if (populateEventGroupReferenceId) {
          eventGroupReferenceId = "some-event-group-reference-id"
        }
        modelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLine1"
        interval = interval {
          if (startSeconds != null) {
            startTime = timestamp {
              seconds = startSeconds
              nanos = 0
            }
          }
          if (endSeconds != null) {
            endTime = timestamp {
              seconds = endSeconds
              nanos = 0
            }
          }
        }
        entityKeys += entityKeyGroups
      }

      val filename =
        when (encoding) {
          BlobEncoding.PROTO -> "metadata-$index.binpb"
          BlobEncoding.JSON -> "metadata-$index.json"
          BlobEncoding.EMPTY -> "metadata-$index"
        }
      val key = "$prefix$filename"

      val bytes = details.serialize(encoding)
      storageClient.writeBlob(key, bytes)
      if (createImpressionFile) {
        storageClient.writeBlob(objectKey, emptyFlow())
      }

      written += key
    }

    // Create done blob and data file at model-line path for gap checking
    val donePrefix = if (edpImpressionPath.isEmpty()) "" else "$edpImpressionPath/"
    val donePath = "${donePrefix}model-line/modelLine1/2026-03-15/done"
    val dataPath = "${donePrefix}model-line/modelLine1/2026-03-15/data_campaign_1"
    storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
    storageClient.writeBlob(donePath, ByteString.copyFromUtf8("done"))

    return written
  }

  private suspend fun seedBlobDetailsWithModelLine(
    storageClient: StorageClient,
    prefix: String,
    intervals: List<Pair<Long?, Long?>>,
    modelLine: String,
    encoding: BlobEncoding = BlobEncoding.PROTO,
    createImpressionFile: Boolean = true,
  ) {
    require(prefix.isEmpty() || prefix.endsWith("/")) { "prefix should end with '/'" }

    intervals.forEachIndexed { index, (startSeconds, endSeconds) ->
      val blobUri = "$bucket/${prefix}some_blob_uri_$index"
      val objectKey = "${prefix}some_blob_uri_$index"
      val details = blobDetails {
        this.blobUri = blobUri
        eventGroupReferenceId = "some-event-group-reference-id"
        this.modelLine = modelLine
        interval = interval {
          if (startSeconds != null) {
            startTime = timestamp {
              seconds = startSeconds
              nanos = 0
            }
          }
          if (endSeconds != null) {
            endTime = timestamp {
              seconds = endSeconds
              nanos = 0
            }
          }
        }
      }

      val filename =
        when (encoding) {
          BlobEncoding.PROTO -> "metadata-$index.binpb"
          BlobEncoding.JSON -> "metadata-$index.json"
          BlobEncoding.EMPTY -> "metadata-$index"
        }
      val key = "$prefix$filename"

      val bytes = details.serialize(encoding)
      storageClient.writeBlob(key, bytes)
      if (createImpressionFile) {
        storageClient.writeBlob(objectKey, emptyFlow())
      }
    }

    // Create done blob and data file at model-line path for gap checking
    val edpImpressionPath = "edp/edpa_edp"
    val modelLineId = modelLine.substringAfterLast("/")
    val donePath = "$edpImpressionPath/model-line/$modelLineId/2026-03-15/done"
    val dataPath = "$edpImpressionPath/model-line/$modelLineId/2026-03-15/data_campaign_1"
    storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
    storageClient.writeBlob(donePath, ByteString.copyFromUtf8("done"))
  }

  private fun BlobDetails.serialize(encoding: BlobEncoding): ByteString =
    when (encoding) {
      BlobEncoding.PROTO -> ByteString.copyFrom(this.toByteArray())
      BlobEncoding.JSON -> ByteString.copyFromUtf8(JsonFormat.printer().print(this))
      BlobEncoding.EMPTY -> ByteString.copyFrom(this.toByteArray())
    }

  /**
   * Seeds a directory with BlobDetails files where the blob_uri points to a custom path.
   *
   * This is useful for testing that the impressions blob key is taken from BlobDetails.blob_uri
   * rather than being inferred from the metadata file path.
   */
  private suspend fun seedBlobDetailsWithCustomImpressionsPath(
    storageClient: StorageClient,
    prefix: String,
    intervals: List<Pair<Long, Long>>,
    impressionsBlobKey: String,
    encoding: BlobEncoding = BlobEncoding.PROTO,
  ) {
    require(prefix.isEmpty() || prefix.endsWith("/")) { "prefix should end with '/'" }

    intervals.forEachIndexed { index, (startSeconds, endSeconds) ->
      // The blob_uri in BlobDetails points to the custom impressions path
      val blobUri = "$bucket/$impressionsBlobKey"
      val details = blobDetails {
        this.blobUri = blobUri
        eventGroupReferenceId = "some-event-group-reference-id"
        modelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLine1"
        interval = interval {
          startTime = timestamp {
            seconds = startSeconds
            nanos = 0
          }
          endTime = timestamp {
            seconds = endSeconds
            nanos = 0
          }
        }
      }

      val filename =
        when (encoding) {
          BlobEncoding.PROTO -> "metadata-$index.binpb"
          BlobEncoding.JSON -> "metadata-$index.json"
          BlobEncoding.EMPTY -> "metadata-$index"
        }
      val metadataKey = "$prefix$filename"

      val bytes = details.serialize(encoding)
      storageClient.writeBlob(metadataKey, bytes)
      // Create the impressions file at the custom path
      storageClient.writeBlob(impressionsBlobKey, emptyFlow())
    }

    // Create done blob and data file at model-line path for gap checking
    val edpImpPath = "edp/edpa_edp"
    val donePath = "$edpImpPath/model-line/modelLine1/2026-03-15/done"
    val dataPath = "$edpImpPath/model-line/modelLine1/2026-03-15/data_campaign_1"
    storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
    storageClient.writeBlob(donePath, ByteString.copyFromUtf8("done"))
  }

  private class RecordingThrottler : Throttler {
    var onReadyCalls: Int = 0

    override suspend fun <T> onReady(block: suspend () -> T): T {
      onReadyCalls++
      return block()
    }
  }

  /**
   * Fake implementation of [BlobMetadataStorageClient] for testing.
   *
   * Wraps a [FileSystemStorageClient] and records all [updateBlobMetadata] calls for verification.
   */
  private class FakeBlobMetadataStorageClient(private val delegate: FileSystemStorageClient) :
    BlobMetadataStorageClient, StorageClient by delegate {

    data class UpdateBlobMetadataCall(
      val blobKey: String,
      val customCreateTime: java.time.Instant?,
      val metadata: Map<String, String>,
    )

    val updateBlobMetadataCalls = mutableListOf<UpdateBlobMetadataCall>()

    override suspend fun updateBlobMetadata(
      blobKey: String,
      customCreateTime: java.time.Instant?,
      metadata: Map<String, String>,
    ) {
      updateBlobMetadataCalls.add(UpdateBlobMetadataCall(blobKey, customCreateTime, metadata))
    }
  }
}
