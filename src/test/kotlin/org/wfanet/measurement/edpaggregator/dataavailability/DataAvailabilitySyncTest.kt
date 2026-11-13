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
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ClientInterceptors
import io.grpc.MethodDescriptor
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
import java.time.Instant
import java.time.LocalDate
import java.util.concurrent.TimeUnit
import java.util.logging.Handler
import java.util.logging.LogRecord
import java.util.logging.Logger
import kotlin.test.assertFailsWith
import kotlin.test.fail
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
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
import org.wfanet.measurement.edpaggregator.v1alpha.BatchUndeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchUpdateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponseKt.modelLineBoundMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.EntityKeyGroup
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchUndeleteImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchUpdateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.entityKey
import org.wfanet.measurement.edpaggregator.v1alpha.entityKeyGroup
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.BlobMetadataStorageClient
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

enum class BlobEncoding {
  PROTO,
  JSON,
  EMPTY,
}

private const val DONE_BLOB_GENERATION = 77L

private suspend fun DataAvailabilitySync.sync(doneBlobPath: String): DataAvailabilitySync.Outcome =
  sync(doneBlobPath, TestDataAvailabilitySyncLease.NAME, DONE_BLOB_GENERATION) {}

private suspend fun DataAvailabilitySync.sync(
  doneBlobPath: String,
  doneBlobGeneration: Long,
): DataAvailabilitySync.Outcome =
  sync(doneBlobPath, TestDataAvailabilitySyncLease.NAME, doneBlobGeneration) {}

private suspend fun DataAvailabilitySync.sync(
  doneBlobPath: String,
  ensureLeaseActive: suspend () -> Unit,
): DataAvailabilitySync.Outcome =
  sync(
    doneBlobPath,
    TestDataAvailabilitySyncLease.NAME,
    DONE_BLOB_GENERATION,
    ensureLeaseActive = ensureLeaseActive,
  )

private object TestDataAvailabilitySyncLease {
  const val NAME =
    "dataProviders/test/dataAvailabilitySyncLeases/11111111-1111-4111-8111-111111111111"
}

@RunWith(JUnit4::class)
class DataAvailabilitySyncTest {
  private val logRecords = mutableListOf<LogRecord>()
  private val rootLogger = Logger.getLogger("")
  private val logHandler =
    object : Handler() {
      override fun publish(record: LogRecord) {
        logRecords += record
      }

      override fun flush() {}

      override fun close() {}
    }

  @Before
  fun setUpLogging() {
    logRecords.clear()
    rootLogger.addHandler(logHandler)
  }

  @After
  fun tearDownLogging() {
    rootLogger.removeHandler(logHandler)
  }

  private val bucket = "file:///my-bucket"
  private val folderPrefix = "edp/edpa_edp/timestamp/"
  private val existingRawImpressionUpload =
    "dataProviders/dataProvider123/rawImpressionUploads/upload-1"
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
    private const val SNAPSHOT_VALIDATION_FAILURES_METRIC =
      "edpa.data_availability.snapshot_validation_failures"
    private const val DATE_COUNT_METRIC = "edpa.data_availability.date_count"
    private const val DEFAULT_BATCH_SIZE = 100
    private const val RAW_IMPRESSION_UPLOAD =
      "dataProviders/dataProvider123/rawImpressionUploads/upload-1"
    private const val MODEL_LINE =
      "modelProviders/provider1/modelSuites/suite1/modelLines/modelLine1"
  }

  private val dataProvidersServiceMock: DataProvidersCoroutineImplBase = mockService {
    onBlocking { replaceDataAvailabilityIntervals(any<ReplaceDataAvailabilityIntervalsRequest>()) }
      .thenAnswer { DataProvider.getDefaultInstance() }
  }

  private val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenAnswer { listImpressionMetadataResponse {} }
      onBlocking { batchCreateImpressionMetadata(any<BatchCreateImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<BatchCreateImpressionMetadataRequest>(0)
          batchCreateImpressionMetadataResponse {
            impressionMetadata +=
              request.requestsList.mapIndexed { index, createRequest ->
                createRequest.impressionMetadata.copy {
                  name = "${request.parent}/impressionMetadata/im-$index"
                }
              }
          }
        }
      onBlocking { batchUpdateImpressionMetadata(any<BatchUpdateImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<BatchUpdateImpressionMetadataRequest>(0)
          batchUpdateImpressionMetadataResponse {
            impressionMetadata +=
              request.requestsList.mapIndexed { index, updateRequest ->
                // Return the input metadata with a name set (simulating server response)
                updateRequest.impressionMetadata.copy {
                  name = "${request.parent}/impressionMetadata/im-$index"
                }
              }
          }
        }
      onBlocking { batchUndeleteImpressionMetadata(any<BatchUndeleteImpressionMetadataRequest>()) }
        .thenAnswer { batchUndeleteImpressionMetadataResponse {} }
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
  fun `sync applies configured Kingdom publication timeout`() = runBlocking {
    val storageClient =
      FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))
    var rpcHadDeadline = false
    val deadlineCapturingChannel =
      ClientInterceptors.intercept(
        grpcTestServerRule.channel,
        object : ClientInterceptor {
          override fun <ReqT : Any, RespT : Any> interceptCall(
            method: MethodDescriptor<ReqT, RespT>,
            callOptions: CallOptions,
            next: Channel,
          ): ClientCall<ReqT, RespT> {
            if (method.fullMethodName.endsWith("/ReplaceDataAvailabilityIntervals")) {
              rpcHadDeadline = callOptions.deadline != null
            }
            return next.newCall(method, callOptions)
          }
        },
      )
    val deadlineApplyingStub = spy(DataProvidersCoroutineStub(deadlineCapturingChannel))
    val publicationTimeout = Duration.ofMinutes(2)
    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        deadlineApplyingStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        RecordingThrottler(),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
        publishEventGroupActivities = null,
        kingdomPublicationTimeout = publicationTimeout,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    dataAvailabilitySync.sync(
      "$bucket/${folderPrefix}done",
      TestDataAvailabilitySyncLease.NAME,
      DONE_BLOB_GENERATION,
    ) {}

    verify(deadlineApplyingStub)
      .withDeadlineAfter(publicationTimeout.toNanos(), TimeUnit.NANOSECONDS)
    assertThat(rpcHadDeadline).isTrue()
  }

  @Test
  fun `external sync rejects overwritten impressions after Kingdom throttle wait`() = runBlocking {
    val storageClient = VersionedBlobMetadataStorageClient()
    val dateFolder = "edp/edpa_edp/model-line/modelLine1/2026-03-15"
    val dataKey = "labeled/data.recordio"
    val initialTime = Instant.parse("2026-03-15T00:01:00Z")
    val doneTime = initialTime.plusSeconds(60)
    storageClient.writeVersion(dataKey, ByteString.copyFromUtf8("data"), initialTime)
    storageClient.writeVersion(
      "$dateFolder/metadata.binpb",
      externalBlobDetails(dataKey).toByteString(),
      initialTime,
    )
    val done = storageClient.writeVersion("$dateFolder/done", ByteString.EMPTY, doneTime)
    var publishing = false
    val sync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        object : Throttler {
          override suspend fun <T> onReady(block: suspend () -> T): T {
            if (publishing) {
              storageClient.writeVersion(
                dataKey,
                ByteString.copyFromUtf8("overwritten"),
                doneTime.plusSeconds(1),
              )
            }
            return block()
          }
        },
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
        publishEventGroupActivities = null,
        generationMatchedMetadataUpdater = storageClient.generationMatchedMetadataUpdater,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    assertFailsWith<ExternalDataAvailabilitySnapshotChangedException> {
      sync.sync(
        "$bucket/$dateFolder/done",
        TestDataAvailabilitySyncLease.NAME,
        doneBlobGeneration = done.generation,
        externalDoneBlobCreateTime = done.createTime,
        onStage = { publishing = it == DataAvailabilitySync.Stage.KINGDOM_PUBLICATION },
        ensureLeaseActive = {},
      )
    }

    verifyBlocking(dataProvidersServiceMock, never()) { replaceDataAvailabilityIntervals(any()) }
    assertThat(storageClient.currentMetadata(blobKey = "$dateFolder/done"))
      .doesNotContainKey(DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY)
  }

  @Test
  fun `external sync rejects overwritten impressions during publication lease renewal`() =
    runBlocking {
      val storageClient = VersionedBlobMetadataStorageClient()
      val dateFolder = "edp/edpa_edp/model-line/modelLine1/2026-03-15"
      val dataKey = "labeled/data.recordio"
      val initialTime = Instant.parse("2026-03-15T00:01:00Z")
      val doneTime = initialTime.plusSeconds(60)
      storageClient.writeVersion(dataKey, ByteString.copyFromUtf8("data"), initialTime)
      storageClient.writeVersion(
        "$dateFolder/metadata.binpb",
        externalBlobDetails(dataKey).toByteString(),
        initialTime,
      )
      val done = storageClient.writeVersion("$dateFolder/done", ByteString.EMPTY, doneTime)
      var publishing = false
      val sync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          RecordingThrottler(),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
          publishEventGroupActivities = null,
          generationMatchedMetadataUpdater = storageClient.generationMatchedMetadataUpdater,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      assertFailsWith<ExternalDataAvailabilitySnapshotChangedException> {
        sync.sync(
          "$bucket/$dateFolder/done",
          TestDataAvailabilitySyncLease.NAME,
          doneBlobGeneration = done.generation,
          externalDoneBlobCreateTime = done.createTime,
          onStage = { publishing = it == DataAvailabilitySync.Stage.KINGDOM_PUBLICATION },
          ensureLeaseActive = {
            if (publishing) {
              storageClient.writeVersion(
                dataKey,
                ByteString.copyFromUtf8("overwritten"),
                doneTime.plusSeconds(1),
              )
            }
          },
        )
      }

      verifyBlocking(dataProvidersServiceMock, never()) { replaceDataAvailabilityIntervals(any()) }
      assertThat(storageClient.currentMetadata(blobKey = "$dateFolder/done"))
        .doesNotContainKey(DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY)
    }

  @Test
  fun `sync rejects lost lease after Kingdom throttle wait`() = runBlocking {
    val storageClient = VersionedBlobMetadataStorageClient()
    val dateFolder = "edp/edpa_edp/model-line/modelLine1/2026-03-15"
    val dataKey = "labeled/data.recordio"
    val initialTime = Instant.parse("2026-03-15T00:01:00Z")
    val doneTime = initialTime.plusSeconds(60)
    storageClient.writeVersion(dataKey, ByteString.copyFromUtf8("data"), initialTime)
    storageClient.writeVersion(
      "$dateFolder/metadata.binpb",
      externalBlobDetails(dataKey).toByteString(),
      initialTime,
    )
    val done = storageClient.writeVersion("$dateFolder/done", ByteString.EMPTY, doneTime)
    var publishing = false
    var leaseActive = true
    val sync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        object : Throttler {
          override suspend fun <T> onReady(block: suspend () -> T): T {
            if (publishing) {
              leaseActive = false
            }
            return block()
          }
        },
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
        publishEventGroupActivities = null,
        generationMatchedMetadataUpdater = storageClient.generationMatchedMetadataUpdater,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    assertFailsWith<IllegalStateException> {
      sync.sync(
        "$bucket/$dateFolder/done",
        TestDataAvailabilitySyncLease.NAME,
        doneBlobGeneration = done.generation,
        externalDoneBlobCreateTime = done.createTime,
        onStage = { publishing = it == DataAvailabilitySync.Stage.KINGDOM_PUBLICATION },
        ensureLeaseActive = { check(leaseActive) },
      )
    }

    verifyBlocking(dataProvidersServiceMock, never()) { replaceDataAvailabilityIntervals(any()) }
    assertThat(storageClient.currentMetadata(blobKey = "$dateFolder/done"))
      .doesNotContainKey(DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY)
  }

  @Test
  fun `external folder scan rejects an object created after done`() = runBlocking {
    val storageClient = VersionedBlobMetadataStorageClient()
    val dateFolder = "edp/edpa_edp/model-line/modelLine1/2026-03-15"
    val doneKey = "$dateFolder/done"
    val dataKey = "labeled/data.recordio"
    val metadataKey = "$dateFolder/metadata.binpb"
    val doneTime = Instant.parse("2026-03-15T00:02:00Z")
    val lateTime = Instant.parse("2026-03-15T00:03:00Z")
    val done = storageClient.writeVersion(doneKey, ByteString.EMPTY, doneTime)
    storageClient.writeVersion(dataKey, ByteString.copyFromUtf8("data"), lateTime)
    storageClient.writeVersion(metadataKey, externalBlobDetails(dataKey).toByteString(), lateTime)

    assertFailsWith<ExternalDataAvailabilitySnapshotChangedException> {
      newExternalDataAvailabilitySync(storageClient)
        .sync(
          "$bucket/$doneKey",
          TestDataAvailabilitySyncLease.NAME,
          doneBlobGeneration = done.generation,
          externalDoneBlobCreateTime = done.createTime,
          ensureLeaseActive = {},
        )
    }

    verifyBlocking(impressionMetadataServiceMock, never()) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(dataProvidersServiceMock, never()) { replaceDataAvailabilityIntervals(any()) }
    assertThat(storageClient.metadataUpdates).isEmpty()
  }

  @Test
  fun `external folder scan rejects late objects and a newer done generation converges`() =
    runBlocking {
      val storageClient = VersionedBlobMetadataStorageClient()
      val dateFolder = "edp/edpa_edp/model-line/modelLine1/2026-03-15"
      val doneKey = "$dateFolder/done"
      val dataAKey = "labeled/data-a.recordio"
      val dataBKey = "labeled/data-b.recordio"
      val metadataAKey = "$dateFolder/metadata-a.binpb"
      val metadataBKey = "$dateFolder/metadata-b.binpb"
      val initialTime = Instant.parse("2026-03-15T00:01:00Z")
      val firstDoneTime = Instant.parse("2026-03-15T00:02:00Z")
      val lateTime = Instant.parse("2026-03-15T00:03:00Z")
      val secondDoneTime = Instant.parse("2026-03-15T00:04:00Z")
      storageClient.writeVersion(dataAKey, ByteString.copyFromUtf8("data-a-v1"), initialTime)
      storageClient.writeVersion(
        metadataAKey,
        externalBlobDetails(dataAKey).toByteString(),
        initialTime,
      )
      val firstDone = storageClient.writeVersion(doneKey, ByteString.EMPTY, firstDoneTime)
      val metricsEnv = createMetricsEnvironment()
      val dataAvailabilitySync =
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ZERO),
          impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
          publishEventGroupActivities = null,
          metrics = metricsEnv.metrics,
          generationMatchedMetadataUpdater = storageClient.generationMatchedMetadataUpdater,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )
      var lateObjectsWritten = false

      try {
        assertFailsWith<ExternalDataAvailabilitySnapshotChangedException> {
          dataAvailabilitySync.sync(
            "$bucket/$doneKey",
            TestDataAvailabilitySyncLease.NAME,
            doneBlobGeneration = firstDone.generation,
            externalDoneBlobCreateTime = firstDone.createTime,
            onStage = { stage ->
              if (stage == DataAvailabilitySync.Stage.METADATA_PERSISTENCE && !lateObjectsWritten) {
                storageClient.writeVersion(dataAKey, ByteString.copyFromUtf8("data-a-v2"), lateTime)
                storageClient.writeVersion(dataBKey, ByteString.copyFromUtf8("data-b"), lateTime)
                storageClient.writeVersion(
                  metadataBKey,
                  externalBlobDetails(dataBKey).toByteString(),
                  lateTime,
                )
                lateObjectsWritten = true
              }
            },
            ensureLeaseActive = {},
          )
        }

        verifyBlocking(impressionMetadataServiceMock, never()) {
          batchCreateImpressionMetadata(any())
        }
        verifyBlocking(dataProvidersServiceMock, never()) {
          replaceDataAvailabilityIntervals(any())
        }
        assertThat(storageClient.metadataUpdates).isEmpty()

        val secondDone = storageClient.writeVersion(doneKey, ByteString.EMPTY, secondDoneTime)
        val outcome =
          dataAvailabilitySync.sync(
            "$bucket/$doneKey",
            TestDataAvailabilitySyncLease.NAME,
            doneBlobGeneration = secondDone.generation,
            externalDoneBlobCreateTime = secondDone.createTime,
            ensureLeaseActive = {},
          )

        assertThat(outcome).isEqualTo(DataAvailabilitySync.Outcome.PUBLISHED)
        val createRequest = argumentCaptor<BatchCreateImpressionMetadataRequest>()
        verifyBlocking(impressionMetadataServiceMock) {
          batchCreateImpressionMetadata(createRequest.capture())
        }
        assertThat(createRequest.firstValue.requestsList.map { it.impressionMetadata.blobUri })
          .containsExactly("$bucket/$metadataAKey", "$bucket/$metadataBKey")
        val registered =
          createRequest.firstValue.requestsList
            .map { it.impressionMetadata }
            .associateBy { it.blobUri }
        assertThat(registered.getValue("$bucket/$metadataAKey").metadataBlobGeneration)
          .isEqualTo(storageClient.currentGeneration(metadataAKey))
        assertThat(registered.getValue("$bucket/$metadataAKey").impressionsBlobGeneration)
          .isEqualTo(storageClient.currentGeneration(dataAKey))
        assertThat(registered.getValue("$bucket/$metadataBKey").metadataBlobGeneration)
          .isEqualTo(storageClient.currentGeneration(metadataBKey))
        assertThat(registered.getValue("$bucket/$metadataBKey").impressionsBlobGeneration)
          .isEqualTo(storageClient.currentGeneration(dataBKey))
        verifyBlocking(dataProvidersServiceMock) { replaceDataAvailabilityIntervals(any()) }
        assertThat(storageClient.metadataUpdates.map { it.generation }.distinct())
          .containsExactly(
            secondDone.generation,
            storageClient.currentGeneration(metadataAKey),
            storageClient.currentGeneration(metadataBKey),
            storageClient.currentGeneration(dataAKey),
            storageClient.currentGeneration(dataBKey),
          )

        assertFailsWith<ExternalDataAvailabilitySnapshotChangedException> {
          dataAvailabilitySync.sync(
            "$bucket/$doneKey",
            TestDataAvailabilitySyncLease.NAME,
            doneBlobGeneration = firstDone.generation,
            externalDoneBlobCreateTime = firstDone.createTime,
            ensureLeaseActive = {},
          )
        }
        verifyBlocking(dataProvidersServiceMock, times(1)) {
          replaceDataAvailabilityIntervals(any())
        }

        metricsEnv.metricReader.forceFlush()
        val validationFailures =
          metricsEnv.metricExporter.finishedMetricItems.single {
            it.name == SNAPSHOT_VALIDATION_FAILURES_METRIC
          }
        assertThat(validationFailures.longSumData.points.single().value).isEqualTo(2L)
      } finally {
        metricsEnv.close()
      }
    }

  @Test
  fun `external folder scan does not stamp a replacement done before persistence`() = runBlocking {
    val storageClient = VersionedBlobMetadataStorageClient()
    val dateFolder = "edp/edpa_edp/model-line/modelLine1/2026-03-15"
    val doneKey = "$dateFolder/done"
    val dataKey = "labeled/data.recordio"
    val metadataKey = "$dateFolder/metadata.binpb"
    val initialTime = Instant.parse("2026-03-15T00:01:00Z")
    val doneTime = Instant.parse("2026-03-15T00:02:00Z")
    val replacementTime = Instant.parse("2026-03-15T00:03:00Z")
    storageClient.writeVersion(dataKey, ByteString.copyFromUtf8("data"), initialTime)
    storageClient.writeVersion(
      metadataKey,
      externalBlobDetails(dataKey).toByteString(),
      initialTime,
    )
    val done = storageClient.writeVersion(doneKey, ByteString.EMPTY, doneTime)
    val dataAvailabilitySync = newExternalDataAvailabilitySync(storageClient)
    var replacementWritten = false

    assertFailsWith<ExternalDataAvailabilitySnapshotChangedException> {
      dataAvailabilitySync.sync(
        "$bucket/$doneKey",
        TestDataAvailabilitySyncLease.NAME,
        doneBlobGeneration = done.generation,
        externalDoneBlobCreateTime = done.createTime,
        onStage = { stage ->
          if (stage == DataAvailabilitySync.Stage.METADATA_PERSISTENCE && !replacementWritten) {
            storageClient.writeVersion(doneKey, ByteString.EMPTY, replacementTime)
            replacementWritten = true
          }
        },
        ensureLeaseActive = {},
      )
    }

    verifyBlocking(impressionMetadataServiceMock, never()) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(dataProvidersServiceMock, never()) { replaceDataAvailabilityIntervals(any()) }
    assertThat(storageClient.currentMetadata(doneKey)).isEmpty()
    assertThat(storageClient.metadataUpdates).isEmpty()
  }

  @Test
  fun `external folder scan does not publish completion on a replacement done`() = runBlocking {
    val storageClient = VersionedBlobMetadataStorageClient()
    val dateFolder = "edp/edpa_edp/model-line/modelLine1/2026-03-15"
    val doneKey = "$dateFolder/done"
    val dataKey = "labeled/data.recordio"
    val metadataKey = "$dateFolder/metadata.binpb"
    val initialTime = Instant.parse("2026-03-15T00:01:00Z")
    val doneTime = Instant.parse("2026-03-15T00:02:00Z")
    val replacementTime = Instant.parse("2026-03-15T00:03:00Z")
    storageClient.writeVersion(dataKey, ByteString.copyFromUtf8("data"), initialTime)
    storageClient.writeVersion(
      metadataKey,
      externalBlobDetails(dataKey).toByteString(),
      initialTime,
    )
    val done = storageClient.writeVersion(doneKey, ByteString.EMPTY, doneTime)
    val dataAvailabilitySync = newExternalDataAvailabilitySync(storageClient)
    var replacement: VersionedBlobMetadataStorageClient.TestBlob? = null
    storageClient.beforeMetadataUpdate = { update ->
      if (
        replacement == null &&
          update.blobKey == doneKey &&
          DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY in update.metadata
      ) {
        replacement = storageClient.writeVersion(doneKey, ByteString.EMPTY, replacementTime)
      }
    }

    assertFailsWith<ExternalDataAvailabilitySnapshotChangedException> {
      dataAvailabilitySync.sync(
        "$bucket/$doneKey",
        TestDataAvailabilitySyncLease.NAME,
        doneBlobGeneration = done.generation,
        externalDoneBlobCreateTime = done.createTime,
        ensureLeaseActive = {},
      )
    }

    val replacementDone = checkNotNull(replacement)
    assertThat(storageClient.currentMetadata(doneKey)).isEmpty()
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }

    storageClient.beforeMetadataUpdate = null
    val outcome =
      dataAvailabilitySync.sync(
        "$bucket/$doneKey",
        TestDataAvailabilitySyncLease.NAME,
        doneBlobGeneration = replacementDone.generation,
        externalDoneBlobCreateTime = replacementDone.createTime,
        ensureLeaseActive = {},
      )

    assertThat(outcome).isEqualTo(DataAvailabilitySync.Outcome.PUBLISHED)
    assertThat(storageClient.currentMetadata(doneKey))
      .containsKey(DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY)
    verifyBlocking(dataProvidersServiceMock, times(2)) { replaceDataAvailabilityIntervals(any()) }
  }

  @Test
  fun `external capture does not reread a missing referenced object recreated after done`() =
    runBlocking {
      val storageClient = VersionedBlobMetadataStorageClient()
      val dateFolder = "edp/edpa_edp/model-line/modelLine1/2026-03-15"
      val doneKey = "$dateFolder/done"
      val dataKey = "labeled/recreated.recordio"
      val metadataKey = "$dateFolder/metadata.binpb"
      val initialTime = Instant.parse("2026-03-15T00:01:00Z")
      val data =
        storageClient.writeVersion(dataKey, ByteString.copyFromUtf8("initial"), initialTime)
      storageClient.writeVersion(
        metadataKey,
        externalBlobDetails(dataKey).toByteString(),
        initialTime,
      )
      val done =
        storageClient.writeVersion(doneKey, ByteString.EMPTY, Instant.parse("2026-03-15T00:02:00Z"))
      data.delete()
      var referencedLookups = 0
      storageClient.afterBlobLookup = { key, blob ->
        if (key == dataKey) {
          referencedLookups++
          if (blob == null) {
            storageClient.writeVersion(
              dataKey,
              ByteString.copyFromUtf8("late"),
              Instant.parse("2026-03-15T00:03:00Z"),
            )
          }
        }
      }
      val sync = newExternalDataAvailabilitySync(storageClient)

      val failure =
        assertFailsWith<IllegalStateException> {
          sync.sync(
            "$bucket/$doneKey",
            TestDataAvailabilitySyncLease.NAME,
            doneBlobGeneration = done.generation,
            externalDoneBlobCreateTime = done.createTime,
            ensureLeaseActive = {},
          )
        }
      assertThat(failure.message).contains("is missing")
      assertThat(referencedLookups).isEqualTo(1)
      assertThat(storageClient.metadataUpdates).isEmpty()
      verifyBlocking(impressionMetadataServiceMock, never()) {
        batchCreateImpressionMetadata(any())
      }
      verifyBlocking(impressionMetadataServiceMock, never()) {
        batchUpdateImpressionMetadata(any())
      }
      verifyBlocking(dataProvidersServiceMock, never()) { replaceDataAvailabilityIntervals(any()) }

      storageClient.afterBlobLookup = null
      val nextDone =
        storageClient.writeVersion(doneKey, ByteString.EMPTY, Instant.parse("2026-03-15T00:04:00Z"))
      assertThat(
          sync.sync(
            "$bucket/$doneKey",
            TestDataAvailabilitySyncLease.NAME,
            doneBlobGeneration = nextDone.generation,
            externalDoneBlobCreateTime = nextDone.createTime,
            ensureLeaseActive = {},
          )
        )
        .isEqualTo(DataAvailabilitySync.Outcome.PUBLISHED)
      val request = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock) {
        batchCreateImpressionMetadata(request.capture())
      }
      assertThat(
          request.firstValue.requestsList.single().impressionMetadata.impressionsBlobGeneration
        )
        .isEqualTo(storageClient.currentGeneration(dataKey))
    }

  @Test
  fun `generationless synchronization cannot clear a metadata generation`() = runBlocking {
    assertGenerationlessSyncRejected(7L, 0L)
  }

  @Test
  fun `generationless synchronization cannot clear an impressions generation`() = runBlocking {
    assertGenerationlessSyncRejected(0L, 8L)
  }

  private suspend fun assertGenerationlessSyncRejected(
    metadataGeneration: Long,
    impressionsGeneration: Long,
  ) {
    val storageClient = FakeBlobMetadataStorageClient(FileSystemStorageClient(tempFolder.root))
    val metadataKey = seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L)).single()
    val existing = impressionMetadata {
      name = "dataProviders/dataProvider123/impressionMetadata/pinned"
      blobUri = "$bucket/$metadataKey"
      blobTypeUrl = "type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.BlobDetails"
      modelLine = MODEL_LINE
      interval = interval {
        startTime = timestamp { seconds = 100 }
        endTime = timestamp { seconds = 200 }
      }
      metadataBlobGeneration = metadataGeneration
      impressionsBlobGeneration = impressionsGeneration
    }
    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(listImpressionMetadataResponse { impressionMetadata += existing })
    val failure =
      assertFailsWith<IllegalStateException> {
        newDataAvailabilitySync(storageClient).sync("$bucket/" + folderPrefix + "done")
      }
    assertThat(failure.message)
      .isEqualTo("Version-pinned metadata requires snapshot-aware synchronization")
    verifyBlocking(impressionMetadataServiceMock, never()) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(impressionMetadataServiceMock, never()) { batchUpdateImpressionMetadata(any()) }
    verifyBlocking(impressionMetadataServiceMock, never()) {
      batchUndeleteImpressionMetadata(any())
    }
    verifyBlocking(dataProvidersServiceMock, never()) { replaceDataAvailabilityIntervals(any()) }
  }

  private fun externalBlobDetails(dataBlobKey: String): BlobDetails = blobDetails {
    blobUri = "$bucket/$dataBlobKey"
    eventGroupReferenceId = "external-event-group"
    modelLine = MODEL_LINE
    interval = interval {
      startTime = timestamp { seconds = 100 }
      endTime = timestamp { seconds = 200 }
    }
  }

  private fun newExternalDataAvailabilitySync(
    storageClient: VersionedBlobMetadataStorageClient
  ): DataAvailabilitySync =
    DataAvailabilitySync(
      "edp/edpa_edp",
      storageClient,
      dataProvidersStub,
      impressionMetadataStub,
      "dataProviders/dataProvider123",
      MinimumIntervalThrottler(Clock.systemUTC(), Duration.ZERO),
      impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
      modelLineMap = emptyMap(),
      errorIfGapsExist = true,
      publishEventGroupActivities = null,
      generationMatchedMetadataUpdater = storageClient.generationMatchedMetadataUpdater,
      availabilityImpressionPaths = listOf("edp/edpa_edp"),
    )

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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    var leaseChecks = 0
    dataAvailabilitySync.sync(
      "$bucket/${folderPrefix}done",
      TestDataAvailabilitySyncLease.NAME,
      doneBlobGeneration = 77L,
    ) {
      leaseChecks++
    }
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    val batchCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchCreateImpressionMetadata(batchCaptor.capture())
    }
    assertThat(batchCaptor.firstValue.requestsCount).isEqualTo(1)
    assertThat(batchCaptor.firstValue.requestsList.single().impressionMetadata.rawImpressionUpload)
      .isEmpty()
    assertThat(
        batchCaptor.firstValue.requestsList.single().impressionMetadata.outputDoneBlobGeneration
      )
      .isEqualTo(77L)
    val boundsRequestCaptor = argumentCaptor<ComputeModelLineBoundsRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      computeModelLineBounds(boundsRequestCaptor.capture())
    }
    assertThat(boundsRequestCaptor.firstValue.parent).isEqualTo("dataProviders/dataProvider123")
    val metadataLog =
      logRecords.single { it.message.contains("event=edpa.data_availability.impression_metadata ") }
    assertThat(metadataLog.message)
      .contains(
        "xmm.edpa.impression_metadata.name=" +
          "dataProviders/dataProvider123/impressionMetadata/im-0"
      )
    assertThat(metadataLog.message)
      .contains(
        "xmm.model_line.name=modelProviders/provider1/modelSuites/suite1/modelLines/modelLine1"
      )
    assertThat(metadataLog.message).contains("xmm.lifecycle.stage=data_availability_metadata")
    assertThat(metadataLog.message).contains("xmm.edpa.impression_metadata.action=created")
    assertThat(metadataLog.message).contains("xmm.outcome=succeeded")
    val intervalLog =
      logRecords.single { it.message.contains("event=edpa.data_availability.interval_published ") }
    assertThat(intervalLog.message)
      .contains(
        "xmm.model_line.name=modelProviders/provider1/modelSuites/suite1/modelLines/modelLineA"
      )
    assertThat(intervalLog.message).contains("xmm.lifecycle.stage=data_availability_publish")
    assertThat(intervalLog.message).contains("xmm.outcome=published")
    assertThat(leaseChecks).isEqualTo(9)
  }

  @Test
  fun `sync publishes EventGroup activity from scanned metadata`() = runBlocking {
    val storageClient =
      FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
    seedBlobDetails(
      storageClient,
      folderPrefix,
      listOf(300L to 400L),
      entityKeyGroups =
        listOf(
          entityKeyGroup {
            entityType = "campaign"
            entityIds += "campaign-1"
          }
        ),
      populateEventGroupReferenceId = false,
    )
    var publishedMetadata: Collection<ImpressionMetadata> = emptyList()
    val publisher: suspend (Collection<ImpressionMetadata>) -> Int = { metadata ->
      publishedMetadata = metadata
      1
    }
    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
        publishEventGroupActivities = publisher,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    val outcome = dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

    assertThat(outcome).isEqualTo(DataAvailabilitySync.Outcome.PUBLISHED)
    assertThat(publishedMetadata).hasSize(1)
    assertThat(publishedMetadata.single().eventGroupReferenceId).isEmpty()
    assertThat(publishedMetadata.single().entityKeysList)
      .containsExactly(
        entityKey {
          entityType = "campaign"
          entityId = "campaign-1"
        }
      )
    verifyBlocking(dataProvidersServiceMock) { replaceDataAvailabilityIntervals(any()) }
    assertThat(
        storageClient.updateBlobMetadataCalls.any {
          DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY in it.metadata
        }
      )
      .isTrue()
  }

  @Test
  fun `sync does not complete publication when EventGroup activity fails`() = runBlocking {
    val storageClient =
      FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))
    val publisher: suspend (Collection<ImpressionMetadata>) -> Int = {
      throw Exception("activity publication failed")
    }
    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
        publishEventGroupActivities = publisher,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    val exception =
      assertFailsWith<Exception> {
        dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
      }

    assertThat(exception).hasMessageThat().isEqualTo("activity publication failed")
    verifyBlocking(dataProvidersServiceMock) { replaceDataAvailabilityIntervals(any()) }
    assertThat(
        storageClient.updateBlobMetadataCalls.none {
          DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY in it.metadata
        }
      )
      .isTrue()
  }

  @Test
  fun `VID Labeler discovery processes only deterministic outputs for the upload`() = runBlocking {
    val storageClient =
      FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
    val eventDate = LocalDate.of(2026, 3, 15)
    val rawBlobUri = "gs://raw-bucket/upload/input.parquet"
    val outputKey = vidLabelerOutputKey(rawBlobUri, eventDate)
    val outputUri = "$bucket/$outputKey"
    val metadataKey = outputKey + ".metadata.binpb"
    val doneBlobUri = vidLabelerDoneUri(eventDate)
    storageClient.writeBlob(outputKey, ByteString.copyFromUtf8("labeled"))
    writeVidLabelerMetadata(storageClient, metadataKey, outputUri, eventDate)
    storageClient.writeBlob(
      "edp/edpa_edp/model-line/modelLine1/$eventDate/unrelated",
      ByteString.copyFromUtf8("unrelated"),
    )
    writeVidLabelerMetadata(
      storageClient,
      "edp/edpa_edp/model-line/modelLine1/$eventDate/unrelated.metadata.binpb",
      "$bucket/edp/edpa_edp/model-line/modelLine1/$eventDate/unrelated",
      eventDate,
    )
    writeVidLabelerDone(storageClient, eventDate)

    val outcome =
      newDataAvailabilitySync(storageClient)
        .sync(
          doneBlobUri,
          TestDataAvailabilitySyncLease.NAME,
          doneBlobGeneration = 77L,
          discoveryMode =
            DataAvailabilitySync.DiscoveryMode.VidLabelerOutputs(
              rawImpressionUpload = RAW_IMPRESSION_UPLOAD,
              rawImpressionBlobUris = listOf(rawBlobUri),
              modelLine = MODEL_LINE,
              eventDate = eventDate,
            ),
          ensureLeaseActive = {},
        )

    assertThat(outcome).isEqualTo(DataAvailabilitySync.Outcome.PUBLISHED)
    val batchCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock) {
      batchCreateImpressionMetadata(batchCaptor.capture())
    }
    val metadata = batchCaptor.firstValue.requestsList.single().impressionMetadata
    assertThat(metadata.blobUri).isEqualTo("$bucket/$metadataKey")
    assertThat(metadata.rawImpressionUpload).isEqualTo(RAW_IMPRESSION_UPLOAD)
    assertThat(metadata.outputDoneBlobGeneration).isEqualTo(77L)
  }

  @Test
  fun `VID Labeler discovery replaces an existing upload lineage with a newer upload`() =
    runBlocking {
      val storageClient =
        FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
      val eventDate = LocalDate.of(2026, 3, 15)
      val rawBlobUri = "gs://raw-bucket/upload/input.parquet"
      val outputKey = vidLabelerOutputKey(rawBlobUri, eventDate)
      val outputUri = "$bucket/$outputKey"
      val metadataKey = outputKey + ".metadata.binpb"
      val oldRawImpressionUpload = "dataProviders/dataProvider123/rawImpressionUploads/upload-old"
      val oldDoneKey =
        "edp/edpa_edp/" +
          LabeledImpressionsBlobKeys.forDone(oldRawImpressionUpload, MODEL_LINE, eventDate)
      storageClient.writeBlob(outputKey, ByteString.copyFromUtf8("labeled"))
      writeVidLabelerMetadata(storageClient, metadataKey, outputUri, eventDate)
      storageClient.writeBlob(oldDoneKey, ByteString.EMPTY)
      val dataAvailabilitySync = newDataAvailabilitySync(storageClient)

      dataAvailabilitySync.sync(
        "$bucket/$oldDoneKey",
        TestDataAvailabilitySyncLease.NAME,
        doneBlobGeneration = 76L,
        discoveryMode =
          DataAvailabilitySync.DiscoveryMode.VidLabelerOutputs(
            rawImpressionUpload = oldRawImpressionUpload,
            rawImpressionBlobUris = listOf(rawBlobUri),
            modelLine = MODEL_LINE,
            eventDate = eventDate,
          ),
        ensureLeaseActive = {},
      )
      val createCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock) {
        batchCreateImpressionMetadata(createCaptor.capture())
      }
      val existing =
        createCaptor.firstValue.requestsList.single().impressionMetadata.copy {
          name = "dataProviders/dataProvider123/impressionMetadata/im-existing"
        }
      wheneverBlocking {
          impressionMetadataServiceMock.listImpressionMetadata(any<ListImpressionMetadataRequest>())
        }
        .thenReturn(listImpressionMetadataResponse { impressionMetadata += existing })
      writeVidLabelerDone(storageClient, eventDate)
      dataAvailabilitySync.sync(
        vidLabelerDoneUri(eventDate),
        TestDataAvailabilitySyncLease.NAME,
        doneBlobGeneration = 77L,
        discoveryMode =
          DataAvailabilitySync.DiscoveryMode.VidLabelerOutputs(
            rawImpressionUpload = RAW_IMPRESSION_UPLOAD,
            rawImpressionBlobUris = listOf(rawBlobUri),
            modelLine = MODEL_LINE,
            eventDate = eventDate,
          ),
        ensureLeaseActive = {},
      )

      val updateCaptor = argumentCaptor<BatchUpdateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock) {
        batchUpdateImpressionMetadata(updateCaptor.capture())
      }
      val updated = updateCaptor.firstValue.requestsList.single().impressionMetadata
      assertThat(updated.name).isEqualTo(existing.name)
      assertThat(updated.rawImpressionUpload).isEqualTo(RAW_IMPRESSION_UPLOAD)
      assertThat(updated.outputDoneBlobGeneration).isEqualTo(77L)
    }

  @Test
  fun `VID Labeler discovery succeeds with no work when every output pair is absent`() =
    runBlocking {
      val storageClient =
        FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
      val eventDate = LocalDate.of(2026, 3, 15)
      writeVidLabelerDone(storageClient, eventDate)
      val doneBlobUri = vidLabelerDoneUri(eventDate)

      val outcome =
        newDataAvailabilitySync(storageClient)
          .sync(
            doneBlobUri,
            TestDataAvailabilitySyncLease.NAME,
            doneBlobGeneration = 77L,
            discoveryMode =
              DataAvailabilitySync.DiscoveryMode.VidLabelerOutputs(
                rawImpressionUpload = RAW_IMPRESSION_UPLOAD,
                rawImpressionBlobUris = listOf("gs://raw-bucket/upload/filtered.parquet"),
                modelLine = MODEL_LINE,
                eventDate = eventDate,
              ),
            ensureLeaseActive = {},
          )

      assertThat(outcome).isEqualTo(DataAvailabilitySync.Outcome.NO_WORK)
      verifyBlocking(impressionMetadataServiceMock, times(0)) {
        batchCreateImpressionMetadata(any())
      }
      val doneMetadata = storageClient.blobMetadata.getValue(vidLabelerDoneKey(eventDate))
      assertThat(doneMetadata)
        .containsEntry(DataAvailabilityBlobs.SYNCED_BY_KEY, DataAvailabilityBlobs.SYNCED_BY_VALUE)
      assertThat(doneMetadata[DataAvailabilityBlobs.SYNC_ID_KEY]).isNotEmpty()
      assertThat(doneMetadata[DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY])
        .isEqualTo(doneMetadata[DataAvailabilityBlobs.SYNC_ID_KEY])
    }

  @Test
  fun `VID Labeler discovery fails when output exists without sidecar`() = runBlocking {
    val storageClient =
      FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
    val eventDate = LocalDate.of(2026, 3, 15)
    val rawBlobUri = "gs://raw-bucket/upload/input.parquet"
    val doneBlobUri = vidLabelerDoneUri(eventDate)
    storageClient.writeBlob(
      vidLabelerOutputKey(rawBlobUri, eventDate),
      ByteString.copyFromUtf8("labeled"),
    )
    writeVidLabelerDone(storageClient, eventDate)

    val error =
      assertFailsWith<IllegalStateException> {
        newDataAvailabilitySync(storageClient)
          .sync(
            doneBlobUri,
            TestDataAvailabilitySyncLease.NAME,
            doneBlobGeneration = 77L,
            discoveryMode =
              DataAvailabilitySync.DiscoveryMode.VidLabelerOutputs(
                rawImpressionUpload = RAW_IMPRESSION_UPLOAD,
                rawImpressionBlobUris = listOf(rawBlobUri),
                modelLine = MODEL_LINE,
                eventDate = eventDate,
              ),
            ensureLeaseActive = {},
          )
      }

    assertThat(error).hasMessageThat().contains("without its metadata sidecar")
  }

  @Test
  fun `VID Labeler discovery fails when sidecar exists without output`() = runBlocking {
    val storageClient =
      FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
    val eventDate = LocalDate.of(2026, 3, 15)
    val rawBlobUri = "gs://raw-bucket/upload/input.parquet"
    val doneBlobUri = vidLabelerDoneUri(eventDate)
    val outputKey = vidLabelerOutputKey(rawBlobUri, eventDate)
    writeVidLabelerMetadata(
      storageClient,
      outputKey + ".metadata.binpb",
      "$bucket/$outputKey",
      eventDate,
    )
    writeVidLabelerDone(storageClient, eventDate)

    val error =
      assertFailsWith<IllegalStateException> {
        newDataAvailabilitySync(storageClient)
          .sync(
            doneBlobUri,
            TestDataAvailabilitySyncLease.NAME,
            doneBlobGeneration = 77L,
            discoveryMode =
              DataAvailabilitySync.DiscoveryMode.VidLabelerOutputs(
                rawImpressionUpload = RAW_IMPRESSION_UPLOAD,
                rawImpressionBlobUris = listOf(rawBlobUri),
                modelLine = MODEL_LINE,
                eventDate = eventDate,
              ),
            ensureLeaseActive = {},
          )
      }

    assertThat(error).hasMessageThat().contains("without its output")
  }

  @Test
  fun `VID Labeler discovery rejects another upload's same-date marker`() = runBlocking {
    val storageClient =
      FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
    val eventDate = LocalDate.of(2026, 3, 15)
    writeVidLabelerDone(storageClient, eventDate)

    val error =
      assertFailsWith<IllegalArgumentException> {
        newDataAvailabilitySync(storageClient)
          .sync(
            vidLabelerDoneUri(eventDate),
            TestDataAvailabilitySyncLease.NAME,
            doneBlobGeneration = 77L,
            discoveryMode =
              DataAvailabilitySync.DiscoveryMode.VidLabelerOutputs(
                rawImpressionUpload = "dataProviders/dp/rawImpressionUploads/other",
                rawImpressionBlobUris = listOf("gs://raw-bucket/upload/input.parquet"),
                modelLine = MODEL_LINE,
                eventDate = eventDate,
              ),
            ensureLeaseActive = {},
          )
      }

    assertThat(error).hasMessageThat().contains("expected upload")
  }

  private fun newDataAvailabilitySync(
    storageClient: BlobMetadataStorageClient,
    publishEventGroupActivities: (suspend (Collection<ImpressionMetadata>) -> Int)? = null,
  ): DataAvailabilitySync =
    DataAvailabilitySync(
      "edp/edpa_edp",
      storageClient,
      dataProvidersStub,
      impressionMetadataStub,
      "dataProviders/dataProvider123",
      MinimumIntervalThrottler(Clock.systemUTC(), Duration.ZERO),
      impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
      modelLineMap = emptyMap(),
      errorIfGapsExist = true,
      publishEventGroupActivities = publishEventGroupActivities,
      availabilityImpressionPaths = listOf("edp/edpa_edp"),
    )

  private fun vidLabelerOutputKey(rawBlobUri: String, eventDate: LocalDate): String =
    "edp/edpa_edp/" + LabeledImpressionsBlobKeys.forInput(rawBlobUri, MODEL_LINE, eventDate)

  private fun vidLabelerDoneKey(
    eventDate: LocalDate,
    rawImpressionUpload: String = RAW_IMPRESSION_UPLOAD,
  ): String =
    "edp/edpa_edp/" + LabeledImpressionsBlobKeys.forDone(rawImpressionUpload, MODEL_LINE, eventDate)

  private fun vidLabelerDoneUri(
    eventDate: LocalDate,
    rawImpressionUpload: String = RAW_IMPRESSION_UPLOAD,
  ): String = "$bucket/${vidLabelerDoneKey(eventDate, rawImpressionUpload)}"

  private suspend fun writeVidLabelerMetadata(
    storageClient: StorageClient,
    metadataKey: String,
    outputUri: String,
    eventDate: LocalDate,
  ) {
    val start = eventDate.atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond()
    storageClient.writeBlob(
      metadataKey,
      blobDetails {
          blobUri = outputUri
          eventGroupReferenceId = "some-event-group-reference-id"
          modelLine = MODEL_LINE
          interval = interval {
            startTime = timestamp { seconds = start }
            endTime = timestamp { seconds = start + 1 }
          }
        }
        .toByteString(),
    )
  }

  private suspend fun writeVidLabelerDone(
    storageClient: StorageClient,
    eventDate: LocalDate,
    rawImpressionUpload: String = RAW_IMPRESSION_UPLOAD,
  ) {
    storageClient.writeBlob(vidLabelerDoneKey(eventDate, rawImpressionUpload), ByteString.EMPTY)
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    val batchCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchCreateImpressionMetadata(batchCaptor.capture())
    }
    assertThat(batchCaptor.firstValue.requestsCount).isEqualTo(1)
    verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }
  }

  @Test
  fun `sync rejects a non-positive done generation`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)
    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ZERO),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    val error =
      assertFailsWith<IllegalArgumentException> {
        dataAvailabilitySync.sync("$bucket/${folderPrefix}done", 0L)
      }

    assertThat(error).hasMessageThat().contains("doneBlobGeneration must be positive")
    verifyBlocking(impressionMetadataServiceMock, never()) { batchCreateImpressionMetadata(any()) }
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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      val requestCaptor = argumentCaptor<ReplaceDataAvailabilityIntervalsRequest>()
      verifyBlocking(dataProvidersServiceMock, times(1)) {
        replaceDataAvailabilityIntervals(requestCaptor.capture())
      }
      val availabilityKeys = requestCaptor.firstValue.dataAvailabilityIntervalsList.map { it.key }
      assertThat(availabilityKeys).containsExactlyElementsIn(mappedLines)
    }
  }

  @Test
  fun `constructor rejects empty mapped model line list`() {
    val storageClient =
      FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
    assertFailsWith<IllegalArgumentException> {
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
        modelLineMap =
          mapOf("modelProviders/provider1/modelSuites/suite1/modelLines/modelLineA" to emptyList()),
        errorIfGapsExist = true,
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )
    }
  }

  @Test
  fun `constructor rejects metadata batch sizes above service limit`() {
    val storageClient =
      FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))

    val exception =
      assertFailsWith<IllegalArgumentException> {
        DataAvailabilitySync(
          "edp/edpa_edp",
          storageClient,
          dataProvidersStub,
          impressionMetadataStub,
          "dataProviders/dataProvider123",
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          impressionMetadataBatchSize = 101,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )
      }

    assertThat(exception).hasMessageThat().contains("must not exceed 100")
  }

  @Test
  fun `sync does not publish when no availability entries are computed`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)
    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))
    wheneverBlocking { impressionMetadataServiceMock.computeModelLineBounds(any()) }
      .thenReturn(computeModelLineBoundsResponse {})
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    assertFailsWith<IllegalStateException> {
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
    }

    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
    val doneMetadata = storageClient.blobMetadata.getValue("${folderPrefix}done")
    assertThat(doneMetadata).containsKey(DataAvailabilityBlobs.SYNC_ID_KEY)
    assertThat(doneMetadata)
      .containsEntry(DataAvailabilityBlobs.SYNCED_BY_KEY, DataAvailabilityBlobs.SYNCED_BY_VALUE)
    assertThat(doneMetadata).doesNotContainKey(DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY)
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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    try {
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    try {
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    try {
      dataAvailabilitySync.sync("$bucket/some-wrong-path/done", DONE_BLOB_GENERATION)
      fail("Expected IllegalArgumentException")
    } catch (e: IllegalArgumentException) {
      // Expected.
    }
  }

  @Test
  fun `sync fails if metadata references missing impression file`() = runBlocking {
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    val exception =
      assertFailsWith<IllegalStateException> {
        dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
      }

    assertThat(exception).hasMessageThat().contains("some_blob_uri_0")
    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(0)) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(0)) { batchUpdateImpressionMetadata(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(0)) {
      batchUndeleteImpressionMetadata(any())
    }
    verifyBlocking(impressionMetadataServiceMock, times(0)) { computeModelLineBounds(any()) }
    assertThat(storageClient.updateBlobMetadataCalls).isEmpty()
  }

  @Test
  fun `sync does not persist present metadata when another referenced file is missing`() =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)
      seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L, 400L to 500L))
      checkNotNull(storageClient.getBlob("${folderPrefix}some_blob_uri_1")).delete()
      var activityPublicationCalls = 0
      val dataAvailabilitySync =
        newDataAvailabilitySync(storageClient) {
          activityPublicationCalls++
          1
        }

      val exception =
        assertFailsWith<IllegalStateException> {
          dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
        }

      assertThat(exception).hasMessageThat().contains("some_blob_uri_1")
      verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
      verifyBlocking(impressionMetadataServiceMock, times(0)) {
        batchCreateImpressionMetadata(any())
      }
      verifyBlocking(impressionMetadataServiceMock, times(0)) {
        batchUpdateImpressionMetadata(any())
      }
      verifyBlocking(impressionMetadataServiceMock, times(0)) {
        batchUndeleteImpressionMetadata(any())
      }
      verifyBlocking(impressionMetadataServiceMock, times(0)) { computeModelLineBounds(any()) }
      assertThat(storageClient.updateBlobMetadataCalls).isEmpty()
      assertThat(activityPublicationCalls).isEqualTo(0)
    }

  @Test
  fun `sync succeeds after referenced file is restored`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)
    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L), createImpressionFile = false)
    val dataAvailabilitySync = newDataAvailabilitySync(storageClient)

    assertFailsWith<IllegalStateException> {
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
    }
    storageClient.writeBlob("${folderPrefix}some_blob_uri_0", emptyFlow())

    val outcome = dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

    assertThat(outcome).isEqualTo(DataAvailabilitySync.Outcome.PUBLISHED)
    verifyBlocking(impressionMetadataServiceMock, times(1)) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
  }

  @Test
  fun `sync returns no work for legitimate empty input`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)
    storageClient.writeBlob("${folderPrefix}done", ByteString.EMPTY)

    val outcome =
      newDataAvailabilitySync(storageClient)
        .sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

    assertThat(outcome).isEqualTo(DataAvailabilitySync.Outcome.NO_WORK)
    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(0)) { batchCreateImpressionMetadata(any()) }
  }

  private suspend fun assertInvalidBlobUriRejected(blobUri: String) {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)
    val details = blobDetails {
      this.blobUri = blobUri
      eventGroupReferenceId = "some-event-group-reference-id"
      modelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLine1"
      interval = interval {
        startTime = timestamp { seconds = 300L }
        endTime = timestamp { seconds = 400L }
      }
    }
    storageClient.writeBlob(
      "${folderPrefix}metadata-invalid-uri.binpb",
      ByteString.copyFrom(details.toByteArray()),
    )

    assertFailsWith<IllegalArgumentException> {
      newDataAvailabilitySync(storageClient)
        .sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
    }
    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(0)) { batchCreateImpressionMetadata(any()) }
  }

  @Test
  fun `sync rejects malformed blob URI`() = runBlocking {
    assertInvalidBlobUriRejected("not a URI")
  }

  @Test
  fun `sync rejects blob URI without object key`() = runBlocking {
    assertInvalidBlobUriRejected("$bucket/")
  }

  @Test
  fun `sync rejects empty blob URI`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)
    val details = blobDetails {
      eventGroupReferenceId = "some-event-group-reference-id"
      modelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLine1"
      interval = interval {
        startTime = timestamp { seconds = 300L }
        endTime = timestamp { seconds = 400L }
      }
    }
    storageClient.writeBlob(
      "${folderPrefix}metadata-empty-uri.binpb",
      ByteString.copyFrom(details.toByteArray()),
    )

    val exception =
      assertFailsWith<IllegalArgumentException> {
        newDataAvailabilitySync(storageClient)
          .sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
      }

    assertThat(exception).hasMessageThat().contains("blob_uri")
    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(0)) { batchCreateImpressionMetadata(any()) }
  }

  @Test
  fun `sync rejects blob URI in another storage root`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)
    val impressionBlobKey = "${folderPrefix}some_blob_uri_0"
    storageClient.writeBlob(impressionBlobKey, emptyFlow())
    val details = blobDetails {
      blobUri = "file:///other-bucket/$impressionBlobKey"
      eventGroupReferenceId = "some-event-group-reference-id"
      modelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLine1"
      interval = interval {
        startTime = timestamp { seconds = 300L }
        endTime = timestamp { seconds = 400L }
      }
    }
    storageClient.writeBlob(
      "${folderPrefix}metadata-other-bucket.binpb",
      ByteString.copyFrom(details.toByteArray()),
    )

    val exception =
      assertFailsWith<IllegalArgumentException> {
        newDataAvailabilitySync(storageClient)
          .sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
      }

    assertThat(exception).hasMessageThat().contains("upload storage root")
    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(0)) { batchCreateImpressionMetadata(any()) }
  }

  @Test
  fun `sync rejects blob URI with another storage scheme`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)
    val impressionBlobKey = "${folderPrefix}some_blob_uri_0"
    storageClient.writeBlob(impressionBlobKey, emptyFlow())
    val details = blobDetails {
      blobUri = "gs://my-bucket/$impressionBlobKey"
      eventGroupReferenceId = "some-event-group-reference-id"
      modelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLine1"
      interval = interval {
        startTime = timestamp { seconds = 300L }
        endTime = timestamp { seconds = 400L }
      }
    }
    storageClient.writeBlob(
      "${folderPrefix}metadata-other-scheme.binpb",
      ByteString.copyFrom(details.toByteArray()),
    )

    val exception =
      assertFailsWith<IllegalArgumentException> {
        newDataAvailabilitySync(storageClient)
          .sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
      }

    assertThat(exception).hasMessageThat().contains("upload storage root")
    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(0)) { batchCreateImpressionMetadata(any()) }
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
          publishEventGroupActivities = null,
          metrics = metricsEnv.metrics,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

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
          publishEventGroupActivities = null,
          metrics = metricsEnv.metrics,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      wheneverBlocking { dataProvidersServiceMock.replaceDataAvailabilityIntervals(any()) }
        .thenAnswer { throw StatusException(Status.UNAVAILABLE) }

      try {
        dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
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
      val intervalFailureLog =
        logRecords.single {
          it.message.contains("event=edpa.data_availability.interval_published ") &&
            it.message.contains("xmm.outcome=failed")
        }
      assertThat(intervalFailureLog.message).contains("xmm.error.code=grpc.UNAVAILABLE")
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `sync retries Kingdom publication after metadata was persisted`(): Unit = runBlocking {
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    wheneverBlocking { dataProvidersServiceMock.replaceDataAvailabilityIntervals(any()) }
      .thenAnswer { throw StatusException(Status.UNAVAILABLE) }

    assertFailsWith<Exception> {
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
    }

    val createCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchCreateImpressionMetadata(createCaptor.capture())
    }
    val persistedMetadata =
      createCaptor.firstValue.requestsList.single().impressionMetadata.copy {
        name = "dataProviders/dataProvider123/impressionMetadata/im-0"
      }
    wheneverBlocking {
        impressionMetadataServiceMock.listImpressionMetadata(any<ListImpressionMetadataRequest>())
      }
      .thenReturn(listImpressionMetadataResponse { impressionMetadata += persistedMetadata })
    wheneverBlocking { dataProvidersServiceMock.replaceDataAvailabilityIntervals(any()) }
      .thenReturn(DataProvider.getDefaultInstance())

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

    verifyBlocking(impressionMetadataServiceMock, times(1)) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(dataProvidersServiceMock, times(2)) { replaceDataAvailabilityIntervals(any()) }
    val doneUpdates = storageClient.updateBlobMetadataCalls.filter { it.blobKey.endsWith("/done") }
    assertThat(doneUpdates).hasSize(5)
    assertThat(doneUpdates.first().metadata).containsKey(DataAvailabilityBlobs.SYNC_ID_KEY)
    assertThat(doneUpdates.first().metadata)
      .doesNotContainKey(DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY)
    assertThat(doneUpdates[1].metadata)
      .containsEntry(DataAvailabilityBlobs.SYNCED_BY_KEY, DataAvailabilityBlobs.SYNCED_BY_VALUE)
    assertThat(doneUpdates[2].metadata[DataAvailabilityBlobs.SYNC_ID_KEY])
      .isNotEqualTo(doneUpdates.first().metadata[DataAvailabilityBlobs.SYNC_ID_KEY])
    assertThat(doneUpdates[3].metadata)
      .containsEntry(DataAvailabilityBlobs.SYNCED_BY_KEY, DataAvailabilityBlobs.SYNCED_BY_VALUE)
    assertThat(doneUpdates.last().metadata).doesNotContainKey(DataAvailabilityBlobs.SYNCED_BY_KEY)
    assertThat(doneUpdates.last().metadata).doesNotContainKey(DataAvailabilityBlobs.SYNC_ID_KEY)
    assertThat(doneUpdates.last().metadata).containsKey(DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY)
    assertThat(doneUpdates.last().metadata[DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY])
      .isEqualTo(doneUpdates[2].metadata[DataAvailabilityBlobs.SYNC_ID_KEY])
  }

  @Test
  fun `sync does not let older completion mask newer attempt`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)
    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))
    val doneBlobKey = "${folderPrefix}done"
    val newerSyncId = "newer-sync-id"
    wheneverBlocking { dataProvidersServiceMock.replaceDataAvailabilityIntervals(any()) }
      .thenAnswer {
        storageClient.mergeBlobMetadata(
          doneBlobKey,
          mapOf(DataAvailabilityBlobs.SYNC_ID_KEY to newerSyncId),
        )
        DataProvider.getDefaultInstance()
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    dataAvailabilitySync.sync("$bucket/$doneBlobKey", DONE_BLOB_GENERATION)

    val doneMetadata = storageClient.blobMetadata.getValue(doneBlobKey)
    assertThat(doneMetadata[DataAvailabilityBlobs.SYNC_ID_KEY]).isEqualTo(newerSyncId)
    assertThat(doneMetadata[DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY]).isNotEqualTo(newerSyncId)
  }

  @Test
  fun `sync announces attempt before metadata persistence`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)
    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L, 500L to 600L))
    wheneverBlocking {
        impressionMetadataServiceMock.batchCreateImpressionMetadata(
          any<BatchCreateImpressionMetadataRequest>()
        )
      }
      .thenThrow(Status.UNAVAILABLE.asRuntimeException())
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    assertFailsWith<Exception> {
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
    }

    val doneMetadata = storageClient.blobMetadata.getValue("${folderPrefix}done")
    assertThat(doneMetadata).containsKey(DataAvailabilityBlobs.SYNC_ID_KEY)
    assertThat(doneMetadata).doesNotContainKey(DataAvailabilityBlobs.SYNCED_BY_KEY)
    assertThat(doneMetadata).doesNotContainKey(DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY)
    val failureLogs =
      logRecords.filter {
        it.message.contains("event=edpa.data_availability.impression_metadata ") &&
          it.message.contains("xmm.outcome=failed")
      }
    assertThat(failureLogs).hasSize(2)
    val pathHashes =
      failureLogs.map {
        checkNotNull(Regex("xmm.gcs.object.path_hash=([0-9a-f]{64})").find(it.message))
          .groupValues[1]
      }
    assertThat(pathHashes.toSet()).hasSize(2)
    failureLogs.forEach {
      assertThat(it.message).contains("xmm.edpa.impression_metadata.action=created")
      assertThat(it.message).doesNotContain("$bucket/")
    }
  }

  @Test
  fun `sync with unchanged legacy sidecar preserves existing raw upload and skips update`() =
    runBlocking {
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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      // First sync — List returns empty, all entries are new -> batchCreate.
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      val createCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchCreateImpressionMetadata(createCaptor.capture())
      }
      val createdMetadata = createCaptor.firstValue.requestsList.single().impressionMetadata

      // Override listImpressionMetadata to return the entries created in the first sync.
      wheneverBlocking {
          impressionMetadataServiceMock.listImpressionMetadata(any<ListImpressionMetadataRequest>())
        }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
          listImpressionMetadataResponse {
            impressionMetadata +=
              createdMetadata.copy {
                name = "${request.parent}/impressionMetadata/im-0"
                rawImpressionUpload = existingRawImpressionUpload
              }
          }
        }

      // Second sync — same content, List returns existing entries -> no create or update.
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      // batchCreate should still have been called exactly once (from the first sync only).
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchCreateImpressionMetadata(any())
      }
      // batchUpdate should never have been called (content unchanged).
      verifyBlocking(impressionMetadataServiceMock, times(0)) {
        batchUpdateImpressionMetadata(any())
      }
    }

  @Test
  fun `sync with unchanged content restamps synced-by marker on metadata blobs`() = runBlocking {
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    // First sync — creates entries and stamps the marker.
    dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

    val createCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchCreateImpressionMetadata(createCaptor.capture())
    }
    val createdMetadata = createCaptor.firstValue.requestsList.single().impressionMetadata

    // List returns the just-created entries so the second sync sees them as existing.
    wheneverBlocking {
        impressionMetadataServiceMock.listImpressionMetadata(any<ListImpressionMetadataRequest>())
      }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
        listImpressionMetadataResponse {
          impressionMetadata +=
            createdMetadata.copy { name = "${request.parent}/impressionMetadata/im-0" }
        }
      }

    // Count metadata-blob stamp calls (those carrying the resource-ID + synced-by metadata)
    // after the first sync.
    val metadataStampCallsAfterFirstSync =
      storageClient.updateBlobMetadataCalls.count {
        it.metadata.containsKey(WatchedBlobs.IMPRESSION_METADATA_RESOURCE_ID_KEY)
      }

    // Second sync — same content, no create or update, but marker must still be restamped.
    dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

    val metadataStampCallsAfterSecondSync =
      storageClient.updateBlobMetadataCalls.count {
        it.metadata.containsKey(WatchedBlobs.IMPRESSION_METADATA_RESOURCE_ID_KEY)
      }

    // Each sync should produce one metadata-stamp call per metadata blob (1 blob in this
    // fixture). If the stamp was gated on create/update responses, the second sync would
    // add zero new calls and this assertion would fail.
    assertThat(metadataStampCallsAfterSecondSync - metadataStampCallsAfterFirstSync).isEqualTo(1)
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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      val captor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(2)) {
        batchCreateImpressionMetadata(captor.capture())
      }
      assertThat(captor.allValues.map { it.requestsCount }).containsExactly(2, 1).inOrder()
      // Two list chunks (3 blob_uris chunked by batch size 2), two create batches, one
      // availability
      // interval update.
      assertThat(recordingThrottler.onReadyCalls).isEqualTo(5)
    }

  @Test
  fun `saveImpressionMetadata batches by flattened entity-key count`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)
    val entityKeyGroups =
      listOf(
        entityKeyGroup {
          entityType = "campaign"
          entityIds += (0 until 532).map { "campaign-$it" }
        },
        entityKeyGroup {
          entityType = "creative"
          entityIds += (0 until 532).map { "creative-$it" }
        },
      )
    seedBlobDetails(
      storageClient,
      folderPrefix,
      (0 until 25).map { index -> (100L + index) to (200L + index) },
      entityKeyGroups = entityKeyGroups,
    )
    val dataAvailabilitySync =
      DataAvailabilitySync(
        "edp/edpa_edp",
        storageClient,
        dataProvidersStub,
        impressionMetadataStub,
        "dataProviders/dataProvider123",
        RecordingThrottler(),
        impressionMetadataBatchSize = 100,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

    val captor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(9)) {
      batchCreateImpressionMetadata(captor.capture())
    }
    assertThat(captor.allValues.map { it.requestsCount })
      .containsExactly(3, 3, 3, 3, 3, 3, 3, 3, 1)
      .inOrder()
    assertThat(
        captor.allValues.map { request ->
          request.requestsList.sumOf { it.impressionMetadata.entityKeysCount }
        }
      )
      .containsExactly(3192, 3192, 3192, 3192, 3192, 3192, 3192, 3192, 1064)
      .inOrder()
    assertThat(captor.allValues.sumOf { it.requestsCount }).isEqualTo(25)
    assertThat(
        captor.allValues.sumOf { request ->
          request.requestsList.sumOf { it.impressionMetadata.entityKeysCount }
        }
      )
      .isEqualTo(26_600)
  }

  @Test
  fun `saveImpressionMetadata update batches include replaced and replacement entity keys`() =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)
      val replacementGroups =
        listOf(
          entityKeyGroup {
            entityType = "campaign"
            entityIds += (0 until 532).map { "new-campaign-$it" }
          },
          entityKeyGroup {
            entityType = "creative"
            entityIds += (0 until 532).map { "new-creative-$it" }
          },
        )
      val intervals = listOf(100L to 200L, 200L to 300L)
      seedBlobDetails(storageClient, folderPrefix, intervals, entityKeyGroups = replacementGroups)
      wheneverBlocking {
          impressionMetadataServiceMock.listImpressionMetadata(any<ListImpressionMetadataRequest>())
        }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
          listImpressionMetadataResponse {
            impressionMetadata +=
              intervals.mapIndexed { index, (startSeconds, endSeconds) ->
                impressionMetadata {
                  name = "${request.parent}/impressionMetadata/im-$index"
                  blobUri = "$bucket/${folderPrefix}metadata-$index.binpb"
                  blobTypeUrl =
                    "type.googleapis.com/wfa.measurement.securecomputation.impressions.BlobDetails"
                  eventGroupReferenceId = "some-event-group-reference-id"
                  modelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLine1"
                  interval = interval {
                    startTime = timestamp { seconds = startSeconds }
                    endTime = timestamp { seconds = endSeconds }
                  }
                  entityKeys +=
                    (0 until 1_064).map { entityIndex ->
                      entityKey {
                        entityType = "legacy"
                        entityId = "legacy-$entityIndex"
                      }
                    }
                  outputDoneBlobGeneration = DONE_BLOB_GENERATION
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
          RecordingThrottler(),
          impressionMetadataBatchSize = 100,
          modelLineMap = emptyMap(),
          errorIfGapsExist = true,
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      val captor = argumentCaptor<BatchUpdateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(2)) {
        batchUpdateImpressionMetadata(captor.capture())
      }
      assertThat(captor.allValues.map { it.requestsCount }).containsExactly(1, 1).inOrder()
      assertThat(
          captor.allValues.map { it.requestsList.single().impressionMetadata.entityKeysCount }
        )
        .containsExactly(1_064, 1_064)
        .inOrder()
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/some-other-edp"),
      )

    assertFailsWith<IllegalArgumentException> {
      runBlocking { dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION) }
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    dataAvailabilitySync.sync(
      "$bucket/edp/edpa_edp/model-line/some-model-line/timestamp/done",
      DONE_BLOB_GENERATION,
    )
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
          outputDoneBlobGeneration = DONE_BLOB_GENERATION
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

    // Five calls: metadata file, impressions file, attempt start on done, metadata-store completion
    // on done, and full availability-publication completion on done.
    assertThat(storageClient.updateBlobMetadataCalls).hasSize(5)

    // Verify metadata file update (has resource ID + synced-by in metadata)
    val metadataFileUpdate =
      storageClient.updateBlobMetadataCalls.single {
        it.metadata.containsKey(WatchedBlobs.IMPRESSION_METADATA_RESOURCE_ID_KEY)
      }
    assertThat(metadataFileUpdate.blobKey).contains("metadata")
    assertThat(metadataFileUpdate.customCreateTime).isNotNull()
    assertThat(metadataFileUpdate.metadata[DataAvailabilityBlobs.SYNCED_BY_KEY])
      .isEqualTo(DataAvailabilityBlobs.SYNCED_BY_VALUE)

    // Verify impressions file update (no metadata, just customCreateTime)
    val impressionsFileUpdate =
      storageClient.updateBlobMetadataCalls.single { it.metadata.isEmpty() }
    assertThat(impressionsFileUpdate.blobKey).contains("some_blob_uri")
    assertThat(impressionsFileUpdate.customCreateTime).isNotNull()

    val doneFileUpdates =
      storageClient.updateBlobMetadataCalls.filter { it.blobKey.endsWith("/done") }
    assertThat(doneFileUpdates).hasSize(3)
    assertThat(doneFileUpdates.first().metadata).containsKey(DataAvailabilityBlobs.SYNC_ID_KEY)
    assertThat(doneFileUpdates.first().metadata)
      .doesNotContainKey(DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY)
    assertThat(doneFileUpdates[1].metadata)
      .containsEntry(DataAvailabilityBlobs.SYNCED_BY_KEY, DataAvailabilityBlobs.SYNCED_BY_VALUE)
    assertThat(doneFileUpdates.last().metadata)
      .doesNotContainKey(DataAvailabilityBlobs.SYNCED_BY_KEY)
    assertThat(doneFileUpdates.last().metadata).doesNotContainKey(DataAvailabilityBlobs.SYNC_ID_KEY)
    assertThat(doneFileUpdates.last().metadata)
      .containsKey(DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY)
    assertThat(doneFileUpdates.last().metadata[DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY])
      .isEqualTo(doneFileUpdates.first().metadata[DataAvailabilityBlobs.SYNC_ID_KEY])
    assertThat(doneFileUpdates.all { it.customCreateTime == null }).isTrue()

    // Metadata and impressions files share the same customCreateTime (interval start time).
    assertThat(metadataFileUpdate.customCreateTime)
      .isEqualTo(impressionsFileUpdate.customCreateTime)
  }

  @Test
  fun `updateBlobMetadata uses impressions blob key from BlobDetails not inferred from metadata URI`() =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      // Create a BlobDetails where blob_uri points to a path that is NOT derived from the
      // metadata
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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

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
        .containsKey(WatchedBlobs.IMPRESSION_METADATA_RESOURCE_ID_KEY)
      assertThat(metadataUpdate.metadata[WatchedBlobs.IMPRESSION_METADATA_RESOURCE_ID_KEY])
        .isEqualTo("dataProviders/dataProvider123/impressionMetadata/im-0")
      assertThat(metadataUpdate.metadata[DataAvailabilityBlobs.SYNCED_BY_KEY])
        .isEqualTo(DataAvailabilityBlobs.SYNCED_BY_VALUE)

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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf(""),
      )

    dataAvailabilitySync.sync(
      "$bucket/edp/edpa_edp/model-line/some-model-line/timestamp/done",
      DONE_BLOB_GENERATION,
    )
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
          outputDoneBlobGeneration = DONE_BLOB_GENERATION
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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf(edpPath),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      // Impression metadata should still be saved
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchCreateImpressionMetadata(any())
      }

      // Model line bounds should still be computed
      verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }

      // replaceDataAvailabilityIntervals should NOT be called due to gaps
      verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
      val gapDecision =
        logRecords.single { it.message.startsWith("event=edpa.data_availability.gap_decision") }
      assertThat(gapDecision.message)
        .contains(
          "xmm.model_line.name=modelProviders/provider1/modelSuites/suite1/" +
            "modelLines/modelLine1"
        )
      assertThat(gapDecision.message).contains("xmm.lifecycle.stage=data_availability_publish")
      assertThat(gapDecision.message).contains("xmm.outcome=blocked")
      assertThat(
          storageClient.updateBlobMetadataCalls.any {
            it.metadata.containsKey(DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY)
          }
        )
        .isFalse()
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf(edpPath),
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

    // replaceDataAvailabilityIntervals should be called since no gaps
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    assertThat(
        logRecords.none { it.message.startsWith("event=edpa.data_availability.gap_decision") }
      )
      .isTrue()
  }

  /**
   * Writes a `done` blob (and, when [withData], a data file) under the model-line path so the gap
   * monitor sees [date] as either finalized (with a "done" blob) or unfinalized (data but no "done"
   * blob). [seedBlobDetails] always finalizes 2026-03-15 for modelLine1, so scenarios are built
   * around that fixed finalized date.
   */
  private suspend fun writeModelLineDate(
    storageClient: StorageClient,
    edpPath: String,
    date: String,
    finalized: Boolean,
    withData: Boolean = true,
  ) {
    val modelLineId = "modelLine1"
    if (withData) {
      val dataPath = "$edpPath/model-line/$modelLineId/$date/data_campaign_1"
      File(tempFolder.root, dataPath).parentFile.mkdirs()
      storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
    }
    if (finalized) {
      val donePath = "$edpPath/model-line/$modelLineId/$date/done"
      File(tempFolder.root, donePath).parentFile.mkdirs()
      storageClient.writeBlob(donePath, ByteString.copyFromUtf8("done"))
    }
  }

  private fun newGapTestSync(
    storageClient: BlobMetadataStorageClient,
    edpPath: String,
    metrics: DataAvailabilitySyncMetrics = DataAvailabilitySyncMetrics(),
    availabilityImpressionPaths: List<String> = listOf(edpPath),
  ) =
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
      publishEventGroupActivities = null,
      metrics = metrics,
      availabilityImpressionPaths = availabilityImpressionPaths,
    )

  @Test
  fun `internal synchronization accepts a missing date finalized under the external root`() =
    runBlocking {
      val storageClient = FakeBlobMetadataStorageClient(FileSystemStorageClient(tempFolder.root))
      val internalPath = "edp/internal"
      val externalPath = "edp/external"
      val selectedFolder = "$internalPath/model-line/modelLine1/2026-03-15/"
      seedBlobDetails(
        storageClient,
        selectedFolder,
        listOf(300L to 400L),
        edpImpressionPath = internalPath,
      )
      writeModelLineDate(storageClient, internalPath, "2026-03-13", finalized = true)
      writeModelLineDate(storageClient, externalPath, "2026-03-14", finalized = true)
      storageClient.writeBlob(
        "$externalPath/model-line/modelLine1/2026-03-14/metadata.binpb",
        externalBlobDetails("unused-external.recordio").toByteString(),
      )

      val outcome =
        multiRootGapSync(storageClient, internalPath, listOf(internalPath, externalPath))
          .sync("$bucket/" + selectedFolder + "done", DONE_BLOB_GENERATION)

      assertThat(outcome).isEqualTo(DataAvailabilitySync.Outcome.PUBLISHED)
      verifyBlocking(dataProvidersServiceMock) { replaceDataAvailabilityIntervals(any()) }
      val request = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock) {
        batchCreateImpressionMetadata(request.capture())
      }
      assertThat(request.firstValue.requestsList.single().impressionMetadata.blobUri)
        .isEqualTo("$bucket/" + selectedFolder + "metadata-0.binpb")
    }

  @Test
  fun `internal synchronization blocks an unfinalized date under the external root`() =
    runBlocking {
      val storageClient = FakeBlobMetadataStorageClient(FileSystemStorageClient(tempFolder.root))
      val internalPath = "edp/internal"
      val externalPath = "edp/external"
      val selectedFolder = "$internalPath/model-line/modelLine1/2026-03-15/"
      seedBlobDetails(
        storageClient,
        selectedFolder,
        listOf(300L to 400L),
        edpImpressionPath = internalPath,
      )
      writeModelLineDate(storageClient, internalPath, "2026-03-13", finalized = true)
      writeModelLineDate(storageClient, externalPath, "2026-03-14", finalized = false)

      val outcome =
        multiRootGapSync(storageClient, internalPath, listOf(internalPath, externalPath))
          .sync("$bucket/" + selectedFolder + "done", DONE_BLOB_GENERATION)

      assertThat(outcome).isEqualTo(DataAvailabilitySync.Outcome.BLOCKED_GAPS)
      verifyBlocking(dataProvidersServiceMock, never()) { replaceDataAvailabilityIntervals(any()) }
    }

  private fun multiRootGapSync(
    storageClient: BlobMetadataStorageClient,
    discoveryPath: String,
    paths: List<String>,
  ) =
    DataAvailabilitySync(
      edpImpressionPath = discoveryPath,
      storageClient = storageClient,
      dataProvidersStub = dataProvidersStub,
      impressionMetadataServiceStub = impressionMetadataStub,
      dataProviderName = "dataProviders/dataProvider123",
      throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ZERO),
      impressionMetadataBatchSize = DEFAULT_BATCH_SIZE,
      modelLineMap = emptyMap(),
      errorIfGapsExist = true,
      publishEventGroupActivities = null,
      availabilityImpressionPaths = paths,
    )

  @Test
  fun `sync blocks a missing date across both impression paths`(): Unit = runBlocking {
    val storageClient =
      FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
    val externalPath = "edp/edpa_edp"
    val internalPath = "vid-labeled-impressions/edp/edpa_edp"
    writeModelLineDate(storageClient, internalPath, "2026-03-13", finalized = true)
    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

    val outcome =
      newGapTestSync(
          storageClient,
          externalPath,
          availabilityImpressionPaths = listOf(externalPath, internalPath),
        )
        .sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

    assertThat(outcome).isEqualTo(DataAvailabilitySync.Outcome.BLOCKED_GAPS)
    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
  }

  @Test
  fun `sync publishes when both impression paths together fill the date range`(): Unit =
    runBlocking {
      val storageClient =
        FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
      val externalPath = "edp/edpa_edp"
      val internalPath = "vid-labeled-impressions/edp/edpa_edp"
      writeModelLineDate(storageClient, externalPath, "2026-03-13", finalized = true)
      writeModelLineDate(storageClient, internalPath, "2026-03-14", finalized = true)
      seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

      val outcome =
        newGapTestSync(
            storageClient,
            externalPath,
            availabilityImpressionPaths = listOf(externalPath, internalPath),
          )
          .sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      assertThat(outcome).isEqualTo(DataAvailabilitySync.Outcome.PUBLISHED)
      verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    }

  @Test
  fun `sync blocks an unfinalized date within the range of both impression paths`(): Unit =
    runBlocking {
      val storageClient =
        FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
      val externalPath = "edp/edpa_edp"
      val internalPath = "vid-labeled-impressions/edp/edpa_edp"
      writeModelLineDate(storageClient, internalPath, "2026-03-13", finalized = true)
      writeModelLineDate(storageClient, internalPath, "2026-03-14", finalized = false)
      seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

      val outcome =
        newGapTestSync(
            storageClient,
            externalPath,
            availabilityImpressionPaths = listOf(externalPath, internalPath),
          )
          .sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      assertThat(outcome).isEqualTo(DataAvailabilitySync.Outcome.BLOCKED_GAPS)
      verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
    }

  @Test
  fun `sync skips replaceDataAvailabilityIntervals when an in-range date has no done blob`(): Unit =
    runBlocking {
      val storageClient =
        FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
      val edpPath = "edp/edpa_edp"

      // Finalized 03-13 and 03-15 (03-15 finalized by seedBlobDetails); 03-14 has data but no
      // "done" blob, so it is unfinalized *inside* the finalized range [03-13, 03-15].
      writeModelLineDate(storageClient, edpPath, "2026-03-13", finalized = true)
      writeModelLineDate(storageClient, edpPath, "2026-03-14", finalized = false)
      seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

      newGapTestSync(storageClient, edpPath)
        .sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      // No true gap exists, but the in-range unfinalized date blocks publishing.
      verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
    }

  @Test
  fun `sync calls replaceDataAvailabilityIntervals when an unfinalized date trails the range`():
    Unit = runBlocking {
    val storageClient =
      FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
    val edpPath = "edp/edpa_edp"

    // Finalized range is [03-13, 03-15] (03-15 from seedBlobDetails). 03-16 has data but no "done"
    // blob, trailing *after* the finalized range, so it must not block publishing.
    writeModelLineDate(storageClient, edpPath, "2026-03-13", finalized = true)
    writeModelLineDate(storageClient, edpPath, "2026-03-14", finalized = true)
    writeModelLineDate(storageClient, edpPath, "2026-03-16", finalized = false)
    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

    val metricsEnv = createMetricsEnvironment()
    try {
      newGapTestSync(storageClient, edpPath, metricsEnv.metrics)
        .sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }

      // Publish proceeds, but the trailing unfinalized date is still surfaced to operators.
      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      assertThat(
          getDateStatusCount(metricData, DataAvailabilityMonitorMetrics.STATUS_WITHOUT_DONE_BLOB)
        )
        .isEqualTo(1)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `sync calls replaceDataAvailabilityIntervals when an unfinalized date leads the range`():
    Unit = runBlocking {
    val storageClient =
      FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
    val edpPath = "edp/edpa_edp"

    // Finalized range is [03-13, 03-15] (03-15 from seedBlobDetails). 03-12 has data but no "done"
    // blob, leading *before* the finalized range, so it must not block publishing.
    writeModelLineDate(storageClient, edpPath, "2026-03-12", finalized = false)
    writeModelLineDate(storageClient, edpPath, "2026-03-13", finalized = true)
    writeModelLineDate(storageClient, edpPath, "2026-03-14", finalized = true)
    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

    val metricsEnv = createMetricsEnvironment()
    try {
      newGapTestSync(storageClient, edpPath, metricsEnv.metrics)
        .sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }

      // Publish proceeds, but the leading-edge unfinalized date is still surfaced to operators.
      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      assertThat(
          getDateStatusCount(metricData, DataAvailabilityMonitorMetrics.STATUS_WITHOUT_DONE_BLOB)
        )
        .isEqualTo(1)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `sync skips replaceDataAvailabilityIntervals when a date is entirely missing`(): Unit =
    runBlocking {
      val storageClient =
        FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
      val edpPath = "edp/edpa_edp"

      // Finalized 03-13 and 03-15 (03-15 from seedBlobDetails); 03-14 is absent entirely, a true
      // gap between two finalized dates.
      writeModelLineDate(storageClient, edpPath, "2026-03-13", finalized = true)
      seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

      newGapTestSync(storageClient, edpPath)
        .sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
    }

  @Test
  fun `sync calls replaceDataAvailabilityIntervals when all dates are finalized and contiguous`():
    Unit = runBlocking {
    val storageClient =
      FakeBlobMetadataStorageClient(FileSystemStorageClient(File(tempFolder.root.toString())))
    val edpPath = "edp/edpa_edp"

    // 03-13, 03-14, 03-15 all finalized and contiguous (03-15 from seedBlobDetails); no gaps and no
    // unfinalized dates.
    writeModelLineDate(storageClient, edpPath, "2026-03-13", finalized = true)
    writeModelLineDate(storageClient, edpPath, "2026-03-14", finalized = true)
    seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

    newGapTestSync(storageClient, edpPath).sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

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
          publishEventGroupActivities = null,
          metrics = metricsEnv.metrics,
          availabilityImpressionPaths = listOf(edpPath),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

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
          publishEventGroupActivities = null,
          metrics = metricsEnv.metrics,
          availabilityImpressionPaths = listOf(edpPath),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf(edpPath),
      )

    // Should NOT throw despite gaps
    dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

    // Impression metadata should still be saved
    verifyBlocking(impressionMetadataServiceMock, times(1)) { batchCreateImpressionMetadata(any()) }

    // Model line bounds should still be computed
    verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }

    // replaceDataAvailabilityIntervals should still be called despite gaps
    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    val gapDecision =
      logRecords.single { it.message.startsWith("event=edpa.data_availability.gap_decision") }
    assertThat(gapDecision.message)
      .contains(
        "xmm.model_line.name=modelProviders/provider1/modelSuites/suite1/" + "modelLines/modelLine1"
      )
    assertThat(gapDecision.message).contains("xmm.lifecycle.stage=data_availability_publish")
    assertThat(gapDecision.message).contains("xmm.outcome=allowed")
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf(edpPath),
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

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
            publishEventGroupActivities = null,
            metrics = metricsEnv.metrics,
            availabilityImpressionPaths = listOf(edpPath),
          )

        dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      assertFailsWith<IllegalArgumentException> {
        dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    assertFailsWith<IllegalArgumentException> {
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    assertFailsWith<IllegalArgumentException> {
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    assertFailsWith<IllegalArgumentException> {
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)
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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      val batchCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchCreateImpressionMetadata(batchCaptor.capture())
      }
      val createdMetadata = batchCaptor.firstValue.requestsList.single().impressionMetadata
      assertThat(createdMetadata.eventGroupReferenceId).isEqualTo("some-event-group-reference-id")
      assertThat(createdMetadata.entityKeysList).isEmpty()
      verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    }

  @Test
  fun `sync creates new, updates changed, and skips unchanged metadata`() = runBlocking {
    val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

    val entityKeyGroups =
      listOf(
        entityKeyGroup {
          entityType = "campaign"
          entityIds += "campaign-1"
        }
      )

    // Seed 3 intervals — all with entity_keys.
    seedBlobDetails(
      storageClient,
      folderPrefix,
      listOf(100L to 200L, 200L to 300L, 300L to 400L),
      entityKeyGroups = entityKeyGroups,
    )

    // Set up list mock to simulate three cases:
    //   100-200: returned WITH entity_keys (matches scan → skip)
    //   200-300: returned WITHOUT entity_keys (differs from scan → update)
    //   300-400: not returned (not found → create)
    wheneverBlocking {
        impressionMetadataServiceMock.listImpressionMetadata(any<ListImpressionMetadataRequest>())
      }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
        val filterBlobUris = request.filter.blobUrisList
        val existing =
          listOf(
            // 100-200: matches scan (has entity_keys) → will be skipped
            impressionMetadata {
              name = "dataProviders/dataProvider123/impressionMetadata/im-0"
              blobUri = "$bucket/${folderPrefix}metadata-0.binpb"
              blobTypeUrl =
                "type.googleapis.com/wfa.measurement.securecomputation.impressions.BlobDetails"
              eventGroupReferenceId = "some-event-group-reference-id"
              modelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLine1"
              interval = interval {
                startTime = timestamp { seconds = 100 }
                endTime = timestamp { seconds = 200 }
              }
              entityKeys +=
                entityKeyGroups.flatMap { group ->
                  group.entityIdsList.map { id ->
                    entityKey {
                      entityType = group.entityType
                      entityId = id
                    }
                  }
                }
              outputDoneBlobGeneration = DONE_BLOB_GENERATION
            },
            // 200-300: differs from scan (no entity_keys) → will be updated
            impressionMetadata {
              name = "dataProviders/dataProvider123/impressionMetadata/im-1"
              blobUri = "$bucket/${folderPrefix}metadata-1.binpb"
              blobTypeUrl =
                "type.googleapis.com/wfa.measurement.securecomputation.impressions.BlobDetails"
              eventGroupReferenceId = "some-event-group-reference-id"
              modelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLine1"
              interval = interval {
                startTime = timestamp { seconds = 200 }
                endTime = timestamp { seconds = 300 }
              }
              // No entity_keys — differs from scan
              rawImpressionUpload = existingRawImpressionUpload
              outputDoneBlobGeneration = DONE_BLOB_GENERATION
            },
            // 300-400: not in list → will be created
          )
        val matching =
          if (filterBlobUris.isEmpty()) existing
          else existing.filter { it.blobUri in filterBlobUris }
        listImpressionMetadataResponse { impressionMetadata += matching }
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
        publishEventGroupActivities = null,
        availabilityImpressionPaths = listOf("edp/edpa_edp"),
      )

    dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

    // Verify batchCreate has only the NEW entry (300-400).
    val createCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchCreateImpressionMetadata(createCaptor.capture())
    }
    val created = createCaptor.firstValue.requestsList.map { it.impressionMetadata }
    assertThat(created).hasSize(1)
    assertThat(created.single().interval.startTime.seconds).isEqualTo(300)
    assertThat(created.single().outputDoneBlobGeneration).isEqualTo(DONE_BLOB_GENERATION)

    // Verify batchUpdate has only the CHANGED entry (200-300).
    val updateCaptor = argumentCaptor<BatchUpdateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchUpdateImpressionMetadata(updateCaptor.capture())
    }
    val updated = updateCaptor.firstValue.requestsList.map { it.impressionMetadata }
    assertThat(updated).hasSize(1)
    assertThat(updated.single().interval.startTime.seconds).isEqualTo(200)
    assertThat(updated.single().outputDoneBlobGeneration).isEqualTo(DONE_BLOB_GENERATION)
    assertThat(updated.single().name)
      .isEqualTo("dataProviders/dataProvider123/impressionMetadata/im-1")
    assertThat(updated.single().entityKeysList).isNotEmpty()
    assertThat(updated.single().rawImpressionUpload).isEqualTo(existingRawImpressionUpload)
  }

  @Test
  fun `overwriting blob_details with entity_keys sends batchUpdate with updated metadata`() =
    runBlocking {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)

      // Step 1: Seed with only event_group_reference_id (no entity_keys) — legacy format.
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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      // First sync — List returns empty, so all entries are new -> batchCreate is called.
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      val firstCaptor = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchCreateImpressionMetadata(firstCaptor.capture())
      }
      val firstMetadata = firstCaptor.firstValue.requestsList.single().impressionMetadata
      assertThat(firstMetadata.eventGroupReferenceId).isEqualTo("some-event-group-reference-id")
      assertThat(firstMetadata.entityKeysList).isEmpty()

      val firstRequestId = firstCaptor.firstValue.requestsList.single().requestId

      // Step 2: Override listImpressionMetadata mock to return the entries created in step 1.
      val createdBlobUri = firstMetadata.blobUri
      wheneverBlocking {
          impressionMetadataServiceMock.listImpressionMetadata(any<ListImpressionMetadataRequest>())
        }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
          val filterBlobUris = request.filter.blobUrisList
          if (filterBlobUris.isEmpty() || filterBlobUris.contains(createdBlobUri)) {
            listImpressionMetadataResponse {
              impressionMetadata +=
                firstMetadata.copy { name = "${request.parent}/impressionMetadata/im-0" }
            }
          } else {
            listImpressionMetadataResponse {}
          }
        }

      // Overwrite the blob_details with entity_keys added.
      val entityKeyGroups =
        listOf(
          entityKeyGroup {
            entityType = "campaign"
            entityIds += "campaign-1"
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

      // Second sync — List returns existing entries, diff finds changed content -> batchUpdate.
      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      val secondCaptor = argumentCaptor<BatchUpdateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchUpdateImpressionMetadata(secondCaptor.capture())
      }
      val secondMetadata = secondCaptor.firstValue.requestsList.single().impressionMetadata
      assertThat(secondMetadata.eventGroupReferenceId).isEqualTo("some-event-group-reference-id")
      assertThat(secondMetadata.entityKeysList)
        .containsExactly(
          entityKey {
            entityType = "campaign"
            entityId = "campaign-1"
          },
          entityKey {
            entityType = "ad"
            entityId = "ad-1"
          },
        )
        .inOrder()

      // The request IDs should differ because entity_keys changed.
      val secondRequestId = secondCaptor.firstValue.requestsList.single().requestId
      assertThat(secondRequestId).isNotEqualTo(firstRequestId)
    }

  @Test
  fun `sync restores deleted impression metadata at the same blob URI`() =
    runBlocking<Unit> {
      val fileSystemClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      val storageClient = FakeBlobMetadataStorageClient(fileSystemClient)
      seedBlobDetails(storageClient, folderPrefix, listOf(300L to 400L))

      var updatedBeforeRestore = false
      wheneverBlocking {
          impressionMetadataServiceMock.listImpressionMetadata(any<ListImpressionMetadataRequest>())
        }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
          assertThat(request.showDeleted).isTrue()
          listImpressionMetadataResponse {
            impressionMetadata += impressionMetadata {
              name = "dataProviders/dataProvider123/impressionMetadata/im-deleted"
              blobUri = "$bucket/${folderPrefix}metadata-0.binpb"
              blobTypeUrl =
                "type.googleapis.com/wfa.measurement.securecomputation.impressions.BlobDetails"
              eventGroupReferenceId = "some-event-group-reference-id"
              modelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLine1"
              interval = interval {
                startTime = timestamp { seconds = 200 }
                endTime = timestamp { seconds = 300 }
              }
              state = org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata.State.DELETED
            }
          }
        }
      wheneverBlocking {
          impressionMetadataServiceMock.batchUpdateImpressionMetadata(
            any<BatchUpdateImpressionMetadataRequest>()
          )
        }
        .thenAnswer { invocation ->
          updatedBeforeRestore = true
          val request = invocation.getArgument<BatchUpdateImpressionMetadataRequest>(0)
          batchUpdateImpressionMetadataResponse {
            impressionMetadata += request.requestsList.map { it.impressionMetadata }
          }
        }
      wheneverBlocking {
          impressionMetadataServiceMock.batchUndeleteImpressionMetadata(
            any<BatchUndeleteImpressionMetadataRequest>()
          )
        }
        .thenAnswer {
          assertThat(updatedBeforeRestore).isTrue()
          batchUndeleteImpressionMetadataResponse {
            impressionMetadata += impressionMetadata {
              name = "dataProviders/dataProvider123/impressionMetadata/im-deleted"
              blobUri = "$bucket/${folderPrefix}metadata-0.binpb"
              modelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLine1"
              interval = interval {
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
          publishEventGroupActivities = null,
          availabilityImpressionPaths = listOf("edp/edpa_edp"),
        )

      dataAvailabilitySync.sync("$bucket/${folderPrefix}done", DONE_BLOB_GENERATION)

      val undeleteCaptor = argumentCaptor<BatchUndeleteImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock) {
        batchUndeleteImpressionMetadata(undeleteCaptor.capture())
      }
      assertThat(undeleteCaptor.firstValue.namesList.single())
        .isEqualTo("dataProviders/dataProvider123/impressionMetadata/im-deleted")
      val updateCaptor = argumentCaptor<BatchUpdateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock) {
        batchUpdateImpressionMetadata(updateCaptor.capture())
      }
      assertThat(
          updateCaptor.firstValue.requestsList
            .single()
            .impressionMetadata
            .interval
            .startTime
            .seconds
        )
        .isEqualTo(300)
      val actionLogs =
        logRecords.filter {
          it.message.contains("event=edpa.data_availability.impression_metadata ") &&
            it.message.contains(
              "xmm.edpa.impression_metadata.name=" +
                "dataProviders/dataProvider123/impressionMetadata/im-deleted"
            )
        }
      assertThat(
          actionLogs.mapNotNull {
            when {
              it.message.contains("xmm.edpa.impression_metadata.action=updated") -> "updated"
              it.message.contains("xmm.edpa.impression_metadata.action=restored") -> "restored"
              else -> null
            }
          }
        )
        .containsExactly("updated", "restored")
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

  private class VersionedBlobMetadataStorageClient :
    BlobMetadataStorageClient, ConditionalOperationStorageClient {
    data class MetadataUpdate(
      val blobKey: String,
      val generation: Long,
      val customCreateTime: Instant?,
      val metadata: Map<String, String>,
    )

    private data class Version(
      val generation: Long,
      val createTime: Instant,
      val updateTime: Instant,
      val content: ByteString,
      val metadata: Map<String, String> = emptyMap(),
    )

    private val versions = linkedMapOf<String, MutableList<Version>>()
    private var nextGeneration = 1L
    val metadataUpdates = mutableListOf<MetadataUpdate>()
    var beforeMetadataUpdate: ((MetadataUpdate) -> Unit)? = null
    var afterBlobLookup: ((String, StorageClient.Blob?) -> Unit)? = null

    val generationMatchedMetadataUpdater =
      GenerationMatchedBlobMetadataUpdater { blobKey, generation, customCreateTime, metadata ->
        updateBlobMetadata(blobKey, generation, customCreateTime, metadata)
      }

    fun writeVersion(blobKey: String, content: ByteString, createTime: Instant): TestBlob {
      val version =
        Version(
          generation = nextGeneration++,
          createTime = createTime,
          updateTime = createTime,
          content = content,
        )
      versions.getOrPut(blobKey) { mutableListOf() } += version
      return TestBlob(blobKey, version)
    }

    fun currentGeneration(blobKey: String): Long =
      checkNotNull(versions[blobKey]?.last()).generation

    fun currentMetadata(blobKey: String): Map<String, String> =
      checkNotNull(versions[blobKey]?.last()).metadata

    override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob =
      writeVersion(
        blobKey,
        content.toList().fold(ByteString.EMPTY, ByteString::concat),
        Instant.now(),
      )

    override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
      val blob = versions[blobKey]?.lastOrNull()?.let { TestBlob(blobKey, it) }
      afterBlobLookup?.invoke(blobKey, blob)
      return blob
    }

    override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> = flow {
      for ((blobKey, history) in versions) {
        if (prefix.isNullOrEmpty() || blobKey.startsWith(prefix)) {
          emit(TestBlob(blobKey, history.last()))
        }
      }
    }

    override suspend fun updateBlobMetadata(
      blobKey: String,
      customCreateTime: Instant?,
      metadata: Map<String, String>,
    ) {
      updateBlobMetadata(blobKey, currentGeneration(blobKey), customCreateTime, metadata)
    }

    private fun updateBlobMetadata(
      blobKey: String,
      generation: Long,
      customCreateTime: Instant?,
      metadata: Map<String, String>,
    ) {
      val update = MetadataUpdate(blobKey, generation, customCreateTime, metadata)
      beforeMetadataUpdate?.invoke(update)
      val history = checkNotNull(versions[blobKey])
      val current = history.last()
      if (current.generation != generation) {
        throw ExternalDataAvailabilitySnapshotChangedException()
      }
      history[history.lastIndex] =
        current.copy(updateTime = Instant.now(), metadata = current.metadata + metadata)
      metadataUpdates += update
    }

    override suspend fun getFreshnessToken(blobKey: String): String? =
      versions[blobKey]?.lastOrNull()?.generation?.toString()

    override suspend fun writeBlobIfUnchanged(
      blobKey: String,
      freshnessToken: String,
      content: Flow<ByteString>,
    ): StorageClient.Blob {
      val current = versions[blobKey]?.lastOrNull()
      if (current?.generation != freshnessToken.toLongOrNull()) {
        throw BlobChangedException("Blob changed")
      }
      return writeVersion(
        blobKey,
        content.toList().fold(ByteString.EMPTY, ByteString::concat),
        Instant.now(),
      )
    }

    override suspend fun writeBlobIfUnchanged(
      blob: StorageClient.Blob,
      content: Flow<ByteString>,
    ): StorageClient.Blob {
      val versionedBlob = blob as TestBlob
      return writeBlobIfUnchanged(blob.blobKey, versionedBlob.freshnessToken, content)
    }

    override suspend fun writeBlobIfNotFound(
      blobKey: String,
      content: Flow<ByteString>,
    ): StorageClient.Blob {
      if (blobKey in versions) throw BlobChangedException("Blob already exists")
      return writeVersion(
        blobKey,
        content.toList().fold(ByteString.EMPTY, ByteString::concat),
        Instant.now(),
      )
    }

    inner class TestBlob(override val blobKey: String, private val version: Version) :
      ConditionalOperationStorageClient.Blob {
      val generation: Long
        get() = version.generation

      override val storageClient: StorageClient
        get() = this@VersionedBlobMetadataStorageClient

      override val freshnessToken: String
        get() = version.generation.toString()

      override val size: Long
        get() = version.content.size().toLong()

      override val createTime: Instant
        get() = version.createTime

      override val updateTime: Instant
        get() = version.updateTime

      override val metadata: Map<String, String>
        get() = version.metadata

      override fun read(): Flow<ByteString> = flowOf(version.content)

      override suspend fun delete() {
        versions.remove(blobKey)
      }
    }
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
    val blobMetadata = mutableMapOf<String, MutableMap<String, String>>()

    fun mergeBlobMetadata(blobKey: String, metadata: Map<String, String>) {
      blobMetadata.getOrPut(blobKey) { mutableMapOf() }.putAll(metadata)
    }

    override suspend fun updateBlobMetadata(
      blobKey: String,
      customCreateTime: java.time.Instant?,
      metadata: Map<String, String>,
    ) {
      updateBlobMetadataCalls.add(UpdateBlobMetadataCall(blobKey, customCreateTime, metadata))
      mergeBlobMetadata(blobKey, metadata)
    }
  }
}
