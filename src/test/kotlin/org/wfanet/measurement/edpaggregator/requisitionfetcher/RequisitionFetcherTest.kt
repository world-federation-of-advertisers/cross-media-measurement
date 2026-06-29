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

package org.wfanet.measurement.edpaggregator.requisitionfetcher

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.StringValue
import com.google.protobuf.timestamp
import com.google.type.interval
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.LongPointData
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import java.time.Clock
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.RefuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.edpaggregator.requisitionfetcher.testing.TestRequisitionData
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.ListRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RefuseRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.listRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class RequisitionFetcherTest {

  @Rule @JvmField val tempFolder = TemporaryFolder()

  private val refuseRequisitionRequests = mutableListOf<RefuseRequisitionRequest>()
  private val createRequisitionMetadataRequests = mutableListOf<CreateRequisitionMetadataRequest>()
  private val refuseRequisitionMetadataRequests = mutableListOf<RefuseRequisitionMetadataRequest>()

  private val requisitionsServiceMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase =
    mockService {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += TestRequisitionData.REQUISITION })
      onBlocking { refuseRequisition(any()) }
        .thenAnswer { invocation ->
          refuseRequisitionRequests += invocation.getArgument<RefuseRequisitionRequest>(0)
          requisition {}
        }
    }

  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { getEventGroup(any()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<GetEventGroupRequest>(0)
        eventGroup {
          name = request.name
          eventGroupReferenceId = "some-event-group-reference-id"
        }
      }
  }

  private val requisitionMetadataServiceMock:
    RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { listRequisitionMetadata(any()) }.thenReturn(listRequisitionMetadataResponse {})
      onBlocking { createRequisitionMetadata(any()) }
        .thenAnswer { invocation ->
          createRequisitionMetadataRequests +=
            invocation.getArgument<CreateRequisitionMetadataRequest>(0)
          requisitionMetadata {
            name = "${TestRequisitionData.EDP_NAME}/requisitionMetadata/m-${System.nanoTime()}"
          }
        }
      onBlocking { refuseRequisitionMetadata(any()) }
        .thenAnswer { invocation ->
          refuseRequisitionMetadataRequests +=
            invocation.getArgument<RefuseRequisitionMetadataRequest>(0)
          requisitionMetadata {}
        }
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(requisitionsServiceMock)
    addService(eventGroupsServiceMock)
    addService(requisitionMetadataServiceMock)
  }

  private val requisitionsStub by lazy {
    RequisitionsGrpcKt.RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }
  private val eventGroupsStub by lazy { EventGroupsCoroutineStub(grpcTestServerRule.channel) }
  private val requisitionMetadataStub by lazy {
    RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  private val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1L))

  private lateinit var storageClient: FileSystemStorageClient
  private lateinit var metricReader: InMemoryMetricReader
  private lateinit var testMetrics: RequisitionFetcherMetrics

  @Before
  fun setUp() {
    storageClient = FileSystemStorageClient(tempFolder.root)
    metricReader = InMemoryMetricReader.create()
    val meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build()
    testMetrics = RequisitionFetcherMetrics(meterProvider.get("test"))
  }

  private fun createFetcher(
    storageClient: StorageClient = this.storageClient,
    metrics: RequisitionFetcherMetrics = testMetrics,
    workerCount: Int = 1,
    maxBufferedRequisitionsPerReport: Int =
      RequisitionFetcher.DEFAULT_MAX_BUFFERED_REQUISITIONS_PER_REPORT,
    metadataThrottler: Throttler = this.throttler,
  ): RequisitionFetcher {
    val validator =
      RequisitionsValidator(
        privateEncryptionKey = TestRequisitionData.EDP_DATA.privateEncryptionKey
      )
    val grouper =
      RequisitionGrouperByReportId(
        requisitionValidator = validator,
        requisitionsClient = requisitionsStub,
        eventGroupsClient = eventGroupsStub,
        kingdomMutationThrottler = throttler,
        kingdomEventGroupThrottler = throttler,
      )
    return RequisitionFetcher(
      requisitionsStub = requisitionsStub,
      requisitionMetadataStub = requisitionMetadataStub,
      storageClient = storageClient,
      dataProviderName = TestRequisitionData.EDP_NAME,
      storagePathPrefix = STORAGE_PATH_PREFIX,
      blobUriPrefix = BLOB_URI_PREFIX,
      requisitionValidator = validator,
      requisitionGrouper = grouper,
      metadataThrottler = metadataThrottler,
      maxBufferedRequisitionsPerReport = maxBufferedRequisitionsPerReport,
      workerCount = workerCount,
      metrics = metrics,
    )
  }

  private fun blobsDir() = tempFolder.root.toPath().resolve(STORAGE_PATH_PREFIX).toFile()

  private fun blobsList() = blobsDir().listFiles().orEmpty()

  @Test
  fun `fetchAndStoreRequisitions writes single grouped blob and creates metadata`() = runBlocking {
    createFetcher().fetchAndStoreRequisitions()

    val files = blobsList()
    assertThat(files).hasLength(1)
    assertThat(createRequisitionMetadataRequests).hasSize(1)
    val groupId = createRequisitionMetadataRequests.single().requisitionMetadata.groupId
    assertThat(files.single().name).isEqualTo(groupId)
  }

  @Test
  fun `same updateTime requisitions for same report produce a single blob`() = runBlocking {
    val r1 = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 10 } }
    val r2 =
      TestRequisitionData.REQUISITION.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
        updateTime = timestamp { seconds = 10 }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += listOf(r1, r2) })

    createFetcher().fetchAndStoreRequisitions()

    assertThat(blobsList()).hasLength(1)
    assertThat(createRequisitionMetadataRequests).hasSize(2)
    val groupIds = createRequisitionMetadataRequests.map { it.requisitionMetadata.groupId }.toSet()
    assertThat(groupIds).hasSize(1)
  }

  @Test
  fun `strictly increasing updateTime closes the buffer and produces two blobs`() = runBlocking {
    val r1 = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 10 } }
    val r2 =
      TestRequisitionData.REQUISITION.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
        updateTime = timestamp { seconds = 20 }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += listOf(r1, r2) })

    createFetcher().fetchAndStoreRequisitions()

    assertThat(blobsList()).hasLength(2)
    val groupIds = createRequisitionMetadataRequests.map { it.requisitionMetadata.groupId }.toSet()
    assertThat(groupIds).hasSize(2)
  }

  @Test
  fun `buffer cap split produces two blobs for one updateTime`() = runBlocking {
    val cap = 2
    val requisitions =
      (1..3).map { idx ->
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo$idx"
          updateTime = timestamp { seconds = 10 }
        }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { this.requisitions += requisitions })

    createFetcher(maxBufferedRequisitionsPerReport = cap).fetchAndStoreRequisitions()

    assertThat(blobsList()).hasLength(2)
    val splitsMetric =
      metricReader.collectAllMetrics().find { it.name == "edpa.requisition_fetcher.buffer_splits" }
    assertThat(splitsMetric).isNotNull()
    val splitsValue = (splitsMetric!!.longSumData.points.first() as LongPointData).value
    assertThat(splitsValue).isEqualTo(1)
  }

  @Test
  fun `writes blob before creating any metadata for that group`() = runBlocking {
    val recordingStorage = OrderRecordingStorageClient(storageClient)
    whenever(requisitionMetadataServiceMock.createRequisitionMetadata(any())).thenAnswer {
      invocation ->
      val request = invocation.getArgument<CreateRequisitionMetadataRequest>(0)
      val groupId = request.requisitionMetadata.groupId
      check(recordingStorage.writtenGroupIds.contains(groupId)) {
        "createRequisitionMetadata called for $groupId before its blob was written"
      }
      createRequisitionMetadataRequests += request
      requisitionMetadata {}
    }

    createFetcher(storageClient = recordingStorage).fetchAndStoreRequisitions()

    assertThat(createRequisitionMetadataRequests).hasSize(1)
  }

  @Test
  fun `crash recovery rewrites missing blob from STORED metadata`() = runBlocking {
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsRequisition = TestRequisitionData.REQUISITION.name
            blobUri = "$BLOB_URI_PREFIX/$STORAGE_PATH_PREFIX/recovered-group-id"
            blobTypeUrl = "type"
            groupId = "recovered-group-id"
            report = "some-report"
          }
        }
      )

    createFetcher().fetchAndStoreRequisitions()

    val recoveredBlob = storageClient.getBlob("$STORAGE_PATH_PREFIX/recovered-group-id")
    assertThat(recoveredBlob).isNotNull()
    val parsed =
      Any.parseFrom(recoveredBlob!!.read().flatten()).unpack(GroupedRequisitions::class.java)
    assertThat(parsed.groupId).isEqualTo("recovered-group-id")
    assertThat(parsed.requisitionsList).hasSize(1)
    assertThat(createRequisitionMetadataRequests).isEmpty()
    val rebuildsMetric =
      metricReader.collectAllMetrics().find {
        it.name == "edpa.requisition_fetcher.recovery_rebuilds"
      }
    assertThat(rebuildsMetric).isNotNull()
    val rebuildsValue = (rebuildsMetric!!.longSumData.points.first() as LongPointData).value
    assertThat(rebuildsValue).isEqualTo(1)
  }

  @Test
  fun `existing blob is not rewritten and metadata for that requisition is skipped`() =
    runBlocking {
      val existingGroupId = "existing-group-id"
      val existingBlobKey = "$STORAGE_PATH_PREFIX/$existingGroupId"
      storageClient.writeBlob(
        existingBlobKey,
        Any.pack(GroupedRequisitions.getDefaultInstance()).toByteString(),
      )
      val originalContent = storageClient.getBlob(existingBlobKey)!!.read().flatten()

      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
        .thenReturn(
          listRequisitionMetadataResponse {
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.STORED
              cmmsRequisition = TestRequisitionData.REQUISITION.name
              blobUri = "$BLOB_URI_PREFIX/$existingBlobKey"
              blobTypeUrl = "type"
              groupId = existingGroupId
              report = "some-report"
            }
          }
        )

      createFetcher().fetchAndStoreRequisitions()

      val afterContent = storageClient.getBlob(existingBlobKey)!!.read().flatten()
      assertThat(afterContent).isEqualTo(originalContent)
      assertThat(createRequisitionMetadataRequests).isEmpty()
    }

  @Test
  fun `requisitions already registered in any metadata state are not re-registered`() =
    runBlocking {
      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
        .thenReturn(
          listRequisitionMetadataResponse {
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.QUEUED
              cmmsRequisition = TestRequisitionData.REQUISITION.name
              blobUri = "$BLOB_URI_PREFIX/$STORAGE_PATH_PREFIX/queued"
              blobTypeUrl = "type"
              groupId = "queued"
              report = "some-report"
            }
          }
        )

      createFetcher().fetchAndStoreRequisitions()

      assertThat(createRequisitionMetadataRequests).isEmpty()
      assertThat(blobsList()).isEmpty()
    }

  @Test
  fun `mixed model lines for one report refuses all requisitions`() = runBlocking {
    val r1 = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 10 } }
    val r2 =
      TestRequisitionData.REQUISITION.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
        measurementSpec =
          signMeasurementSpec(
            TestRequisitionData.MEASUREMENT_SPEC.copy { modelLine = "other-model-line" },
            TestRequisitionData.MC_SIGNING_KEY,
          )
        updateTime = timestamp { seconds = 10 }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += listOf(r1, r2) })

    createFetcher().fetchAndStoreRequisitions()

    assertThat(refuseRequisitionRequests).hasSize(2)
    assertThat(createRequisitionMetadataRequests).hasSize(2)
    assertThat(refuseRequisitionMetadataRequests).hasSize(2)
    assertThat(blobsList()).isEmpty()
  }

  @Test
  fun `unparseable MeasurementSpec is refused without a work unit`() = runBlocking {
    val bad =
      TestRequisitionData.REQUISITION.copy {
        measurementSpec = signedMessage {
          message = Any.pack(StringValue.newBuilder().setValue("x").build())
        }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += bad })

    createFetcher().fetchAndStoreRequisitions()

    assertThat(refuseRequisitionRequests).hasSize(1)
    assertThat(createRequisitionMetadataRequests).isEmpty()
  }

  @Test
  fun `mismatched event group selectors refuses all requisitions for the report`() = runBlocking {
    val secondEventGroupName = "${TestRequisitionData.EDP_NAME}/eventGroups/name2"
    eventGroupsServiceMock.stub {
      onBlocking { getEventGroup(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<GetEventGroupRequest>(0)
          if (request.name == TestRequisitionData.EVENT_GROUP_NAME) {
            eventGroup {
              name = request.name
              eventGroupReferenceId = "ref-1"
              entityKey =
                EventGroupKt.entityKey {
                  entityType = "placement"
                  entityId = "P-1"
                }
            }
          } else {
            eventGroup {
              name = request.name
              eventGroupReferenceId = "ref-2"
            }
          }
        }
    }
    val requisitionSpec =
      TestRequisitionData.REQUISITION_SPEC.copy {
        events =
          RequisitionSpecKt.events {
            eventGroups +=
              RequisitionSpecKt.eventGroupEntry {
                key = TestRequisitionData.EVENT_GROUP_NAME
                value =
                  RequisitionSpecKt.EventGroupEntryKt.value {
                    collectionInterval = interval {
                      startTime = TestRequisitionData.TIME_RANGE.start.toProtoTime()
                      endTime = TestRequisitionData.TIME_RANGE.endExclusive.toProtoTime()
                    }
                  }
              }
            eventGroups +=
              RequisitionSpecKt.eventGroupEntry {
                key = secondEventGroupName
                value =
                  RequisitionSpecKt.EventGroupEntryKt.value {
                    collectionInterval = interval {
                      startTime = TestRequisitionData.TIME_RANGE.start.toProtoTime()
                      endTime = TestRequisitionData.TIME_RANGE.endExclusive.toProtoTime()
                    }
                  }
              }
          }
      }
    val requisitionWithMixed =
      TestRequisitionData.REQUISITION.copy {
        encryptedRequisitionSpec =
          encryptRequisitionSpec(
            signedMessage { message = requisitionSpec.pack() },
            TestRequisitionData.DATA_PROVIDER_PUBLIC_KEY,
          )
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += requisitionWithMixed })

    createFetcher().fetchAndStoreRequisitions()

    assertThat(refuseRequisitionRequests).hasSize(1)
    assertThat(createRequisitionMetadataRequests).hasSize(1)
    assertThat(refuseRequisitionMetadataRequests).hasSize(1)
  }

  @Test
  fun `per-report failure isolation lets other reports succeed`() = runBlocking {
    val r1 = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 10 } }
    val measurementSpec2 =
      TestRequisitionData.MEASUREMENT_SPEC.copy {
        reportingMetadata = MeasurementSpecKt.reportingMetadata { report = "other-report" }
      }
    val r2 =
      TestRequisitionData.REQUISITION.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
        measurementSpec = signMeasurementSpec(measurementSpec2, TestRequisitionData.MC_SIGNING_KEY)
        updateTime = timestamp { seconds = 10 }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += listOf(r1, r2) })
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any())).thenAnswer { invocation
      ->
      val req = invocation.getArgument<ListRequisitionMetadataRequest>(0)
      if (req.filter.report == "other-report") {
        throw RuntimeException("simulated listRequisitionMetadata failure for other-report")
      }
      listRequisitionMetadataResponse {}
    }

    createFetcher(workerCount = 2).fetchAndStoreRequisitions()

    assertThat(blobsList()).hasLength(1)
    val failuresMetric =
      metricReader.collectAllMetrics().find {
        it.name == "edpa.requisition_fetcher.report_failures"
      }
    assertThat(failuresMetric).isNotNull()
    val failuresValue = (failuresMetric!!.longSumData.points.first() as LongPointData).value
    assertThat(failuresValue).isEqualTo(1)
  }

  @Test
  fun `metadata throttler gates each metadata RPC`() = runBlocking {
    val counter = CountingThrottler()

    val r1 = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 10 } }
    val r2 =
      TestRequisitionData.REQUISITION.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
        updateTime = timestamp { seconds = 10 }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += listOf(r1, r2) })

    createFetcher(metadataThrottler = counter).fetchAndStoreRequisitions()

    // 1 list + 2 create metadata calls.
    assertThat(counter.count.get()).isEqualTo(3)
  }

  @Test
  fun `streams more requisitions than channel capacity without deadlock`() = runBlocking {
    val n = 200
    val requisitions =
      (1..n).map { idx ->
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo$idx"
          updateTime = timestamp { seconds = idx.toLong() }
        }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { this.requisitions += requisitions })

    createFetcher(workerCount = 4).fetchAndStoreRequisitions()

    assertThat(blobsList()).hasLength(n)
    assertThat(createRequisitionMetadataRequests).hasSize(n)
  }

  private class CountingThrottler : Throttler {
    val count = AtomicInteger(0)

    override suspend fun <T> onReady(block: suspend () -> T): T {
      count.incrementAndGet()
      return block()
    }
  }

  private class OrderRecordingStorageClient(private val delegate: StorageClient) : StorageClient {
    val writtenGroupIds = mutableListOf<String>()

    override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
      val groupId = blobKey.substringAfterLast('/')
      val blob = delegate.writeBlob(blobKey, content)
      writtenGroupIds += groupId
      return blob
    }

    override suspend fun getBlob(blobKey: String): StorageClient.Blob? = delegate.getBlob(blobKey)

    override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> =
      delegate.listBlobs(prefix)
  }

  companion object {
    init {
      EdpaTelemetry.ensureInitialized()
    }

    private const val STORAGE_PATH_PREFIX = "test-requisitions"
    private const val BLOB_URI_PREFIX = "file:///my-bucket"
  }
}
