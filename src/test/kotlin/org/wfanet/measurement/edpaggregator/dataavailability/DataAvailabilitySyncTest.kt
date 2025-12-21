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
import io.opentelemetry.api.common.AttributeKey
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
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponseKt.modelLineBoundMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
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
            impressionMetadata += request.requestsList.map { it.impressionMetadata }
          }
        }
      onBlocking { computeModelLineBounds(any<ComputeModelLineBoundsRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ComputeModelLineBoundsRequest>(0)
          // Build some fake proto data for the response
          computeModelLineBoundsResponse {
            modelLineBounds += modelLineBoundMapEntry {
              key = "${request.parent}/modelLines/modelLineA"
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
    val meter = meterProvider.get("data-availability-sync-test")
    return MetricsTestEnvironment(
      DataAvailabilitySyncMetrics(meter),
      metricExporter,
      metricReader,
      meterProvider,
    )
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(dataProvidersServiceMock)
    addService(impressionMetadataServiceMock)
  }

  @get:Rule val tempFolder = TemporaryFolder()

  @Test
  fun `register single contiguous day for existing model line using proto message`() = runBlocking {
    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

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
  fun `register single contiguous day for existing model line using json message`() = runBlocking {
    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

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
  fun `register single overlapping day for existing model line`() = runBlocking {
    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

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
      val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

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
    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

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
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(1)) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }
  }

  @Test
  fun `blob details with missing interval throws IllegalArgumentException`() = runBlocking {
    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

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
    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

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
    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

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
    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

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
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done")
    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(0)) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(0)) { computeModelLineBounds(any()) }
  }

  @Test
  fun `sync emits success metrics`() = runBlocking {
    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

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
    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

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
    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

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
      val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

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
    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))
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
      )

    assertFailsWith<IllegalArgumentException> {
      runBlocking { dataAvailabilitySync.sync("$bucket/${folderPrefix}done") }
    }
  }

  @Test
  fun `supports different subfolders`() = runBlocking {
    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

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
          eventGroupReferenceId = "event${index+1}"
          modelLine = "modelLine1"
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
    assertThat(createdItems).isEqualTo(impressionMetadataRequests)
    verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }
  }

  @Test
  fun `works with blank edp impression path`() = runBlocking {
    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

    seedBlobDetails(
      storageClient,
      "edp/edpa_edp/model-line/some-model-line/timestamp/",
      listOf(300L to 400L, 400L to 500L, 500L to 600L),
      BlobEncoding.JSON,
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
          eventGroupReferenceId = "event${index+1}"
          modelLine = "modelLine1"
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
    assertThat(createdItems).isEqualTo(impressionMetadataRequests)
    verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }
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
  ): List<String> {
    require(prefix.isEmpty() || prefix.endsWith("/")) { "prefix should end with '/'" }

    val written = mutableListOf<String>()

    intervals.forEachIndexed { index, (startSeconds, endSeconds) ->
      val blobUri = "$bucket/${folderPrefix}some_blob_uri_$index"
      val objectKey = "${folderPrefix}some_blob_uri_$index"
      val details = blobDetails {
        this.blobUri = blobUri
        eventGroupReferenceId = "event${index + 1}"
        modelLine = "modelLine1"
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

      written += key
    }

    return written
  }

  private fun BlobDetails.serialize(encoding: BlobEncoding): ByteString =
    when (encoding) {
      BlobEncoding.PROTO -> ByteString.copyFrom(this.toByteArray())
      BlobEncoding.JSON -> ByteString.copyFromUtf8(JsonFormat.printer().print(this))
      BlobEncoding.EMPTY -> ByteString.copyFrom(this.toByteArray())
    }

  private class RecordingThrottler : Throttler {
    var onReadyCalls: Int = 0

    override suspend fun <T> onReady(block: suspend () -> T): T {
      onReadyCalls++
      return block()
    }
  }
}
