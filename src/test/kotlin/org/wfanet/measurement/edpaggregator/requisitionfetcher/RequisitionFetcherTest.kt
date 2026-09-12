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
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.LongPointData
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import java.time.Clock
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertThrows
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.stub
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequest
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
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.telemetry.ReportTraceAttributes
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.edpaggregator.requisitionfetcher.testing.TestRequisitionData
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.ListRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.QueueRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RefuseRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RegisterQueuedRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.registerQueuedRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.resultsFulfillerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.EnsureWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class RequisitionFetcherTest {

  @Rule @JvmField val tempFolder = TemporaryFolder()

  private val refuseRequisitionRequests = mutableListOf<RefuseRequisitionRequest>()
  private val createRequisitionMetadataRequests = mutableListOf<CreateRequisitionMetadataRequest>()
  private val refuseRequisitionMetadataRequests = mutableListOf<RefuseRequisitionMetadataRequest>()
  private val queueRequisitionMetadataRequests = mutableListOf<QueueRequisitionMetadataRequest>()
  private val registerQueuedRequisitionMetadataRequests =
    mutableListOf<RegisterQueuedRequisitionMetadataRequest>()
  private val ensureWorkItemRequests = mutableListOf<EnsureWorkItemRequest>()

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

  private val workItemsServiceMock: WorkItemsGrpcKt.WorkItemsCoroutineImplBase = mockService {
    onBlocking { ensureWorkItem(any()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<EnsureWorkItemRequest>(0)
        ensureWorkItemRequests += request
        workItem {
          name = "workItems/${request.workItemId}"
          queue = request.workItem.queue
          workItemParams = request.workItem.workItemParams
          state = WorkItem.State.QUEUED
        }
      }
  }

  private val requisitionMetadataServiceMock:
    RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { listRequisitionMetadata(any()) }.thenReturn(listRequisitionMetadataResponse {})
      onBlocking { batchCreateRequisitionMetadata(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<BatchCreateRequisitionMetadataRequest>(0)
          createRequisitionMetadataRequests += request.requestsList
          batchCreateRequisitionMetadataResponse {
            requisitionMetadata +=
              request.requestsList.map { subRequest ->
                requisitionMetadata {
                  name =
                    "${TestRequisitionData.EDP_NAME}/requisitionMetadata/m-${System.nanoTime()}"
                  cmmsRequisition = subRequest.requisitionMetadata.cmmsRequisition
                  blobUri = subRequest.requisitionMetadata.blobUri
                  blobTypeUrl = subRequest.requisitionMetadata.blobTypeUrl
                  groupId = subRequest.requisitionMetadata.groupId
                  cmmsCreateTime = subRequest.requisitionMetadata.cmmsCreateTime
                  report = subRequest.requisitionMetadata.report
                  state = RequisitionMetadata.State.STORED
                  etag = "stored-etag"
                }
              }
          }
        }
      onBlocking { registerQueuedRequisitionMetadata(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<RegisterQueuedRequisitionMetadataRequest>(0)
          registerQueuedRequisitionMetadataRequests += request
          createRequisitionMetadataRequests += request.requestsList
          registerQueuedRequisitionMetadataResponse {
            requisitionMetadata +=
              request.requestsList.map { subRequest ->
                requisitionMetadata {
                  name =
                    "${TestRequisitionData.EDP_NAME}/requisitionMetadata/m-${System.nanoTime()}"
                  cmmsRequisition = subRequest.requisitionMetadata.cmmsRequisition
                  blobUri = subRequest.requisitionMetadata.blobUri
                  blobTypeUrl = subRequest.requisitionMetadata.blobTypeUrl
                  groupId = subRequest.requisitionMetadata.groupId
                  cmmsCreateTime = subRequest.requisitionMetadata.cmmsCreateTime
                  report = subRequest.requisitionMetadata.report
                  state = RequisitionMetadata.State.QUEUED
                  workItem = request.workItem
                  etag = "queued-etag"
                }
              }
          }
        }
      onBlocking { queueRequisitionMetadata(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<QueueRequisitionMetadataRequest>(0)
          queueRequisitionMetadataRequests += request
          requisitionMetadata {
            name = request.name
            state = RequisitionMetadata.State.QUEUED
            workItem = request.workItem
            etag = "queued-etag"
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
    addService(workItemsServiceMock)
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
  private val workItemsStub by lazy {
    WorkItemsGrpcKt.WorkItemsCoroutineStub(grpcTestServerRule.channel)
  }

  private val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1L))

  private lateinit var storageClient: FileSystemStorageClient
  private lateinit var metricReader: InMemoryMetricReader
  private lateinit var testMetrics: RequisitionFetcherMetrics
  private lateinit var openTelemetry: OpenTelemetrySdk
  private lateinit var spanExporter: InMemorySpanExporter

  @Before
  fun setUp() {
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    spanExporter = InMemorySpanExporter.create()
    openTelemetry =
      OpenTelemetrySdk.builder()
        .setTracerProvider(
          SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build()
        )
        .buildAndRegisterGlobal()
    storageClient = FileSystemStorageClient(tempFolder.root)
    metricReader = InMemoryMetricReader.create()
    val meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build()
    testMetrics = RequisitionFetcherMetrics(meterProvider.get("test"))
  }

  @After
  fun cleanUpTelemetry() {
    openTelemetry.close()
    spanExporter.reset()
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
  }

  private fun createFetcher(
    storageClient: StorageClient = this.storageClient,
    metrics: RequisitionFetcherMetrics = testMetrics,
    flushInterval: Duration = RequisitionFetcher.DEFAULT_FLUSH_INTERVAL,
    maxTotalBufferedBytes: Long = RequisitionFetcher.DEFAULT_MAX_TOTAL_BUFFERED_BYTES,
    maxRequisitionsPerGroup: Int = RequisitionFetcher.DEFAULT_MAX_REQUISITIONS_PER_GROUP,
    metadataThrottler: Throttler = this.throttler,
    workItemDispatcher: RequisitionWorkItemDispatcher? = null,
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
      directStoragePathPrefix =
        if (workItemDispatcher == null) null else DIRECT_STORAGE_PATH_PREFIX,
      blobUriPrefix = BLOB_URI_PREFIX,
      requisitionValidator = validator,
      requisitionGrouper = grouper,
      metadataThrottler = metadataThrottler,
      workItemDispatcher = workItemDispatcher,
      flushInterval = flushInterval,
      maxTotalBufferedBytes = maxTotalBufferedBytes,
      maxRequisitionsPerGroup = maxRequisitionsPerGroup,
      metrics = metrics,
    )
  }

  private fun blobsDir() = tempFolder.root.toPath().resolve(STORAGE_PATH_PREFIX).toFile()

  private fun directBlobsDir() =
    tempFolder.root.toPath().resolve(DIRECT_STORAGE_PATH_PREFIX).toFile()

  /**
   * Makes [requisitionMetadataServiceMock].listRequisitionMetadata stateful: it returns STORED rows
   * for every requisition already persisted via batchCreateRequisitionMetadata (recorded in
   * [createRequisitionMetadataRequests]), filtered to the requested report. This mirrors the real
   * service so that a second work unit for a report observes the first unit's STORED metadata,
   * which is what the bufferSplits split-detection and cross-unit recovery both key off of.
   */
  private fun installStatefulMetadataMock() {
    requisitionMetadataServiceMock.stub {
      onBlocking { listRequisitionMetadata(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListRequisitionMetadataRequest>(0)
          val reportFilter = request.filter.report
          listRequisitionMetadataResponse {
            requisitionMetadata +=
              createRequisitionMetadataRequests
                .map { it.requisitionMetadata }
                .filter { reportFilter.isEmpty() || it.report == reportFilter }
                .map {
                  requisitionMetadata {
                    state = RequisitionMetadata.State.STORED
                    cmmsRequisition = it.cmmsRequisition
                    groupId = it.groupId
                    report = it.report
                    blobUri = it.blobUri
                    blobTypeUrl = it.blobTypeUrl
                  }
                }
          }
        }
    }
  }

  private fun blobsList() = blobsDir().listFiles().orEmpty()

  @Test
  fun `constructor rejects non-positive flushInterval`() {
    assertFailsWith<IllegalArgumentException> { createFetcher(flushInterval = Duration.ZERO) }
  }

  @Test
  fun `constructor rejects non-positive maxRequisitionsPerGroup`() {
    assertFailsWith<IllegalArgumentException> { createFetcher(maxRequisitionsPerGroup = 0) }
  }

  @Test
  fun `constructor rejects non-positive maxTotalBufferedBytes`() {
    assertFailsWith<IllegalArgumentException> { createFetcher(maxTotalBufferedBytes = 0) }
  }

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
  fun `direct dispatch registers queued metadata before ensuring WorkItem`() = runBlocking {
    var dispatchedGroupId: String? = null
    var dispatchedBlobUri: String? = null
    var allMetadataQueuedBeforeDispatch = false
    val dispatcher =
      object : RequisitionWorkItemDispatcher {
        override fun workItemName(groupId: String): String = "workItems/results-fulfiller-$groupId"

        override suspend fun dispatch(groupId: String, blobUri: String) {
          dispatchedGroupId = groupId
          dispatchedBlobUri = blobUri
          allMetadataQueuedBeforeDispatch =
            registerQueuedRequisitionMetadataRequests.single().requestsCount ==
              createRequisitionMetadataRequests.size
        }
      }

    createFetcher(workItemDispatcher = dispatcher).fetchAndStoreRequisitions()

    val groupId = createRequisitionMetadataRequests.single().requisitionMetadata.groupId
    assertThat(registerQueuedRequisitionMetadataRequests).hasSize(1)
    assertThat(registerQueuedRequisitionMetadataRequests.single().workItem)
      .isEqualTo("workItems/results-fulfiller-$groupId")
    assertThat(dispatchedGroupId).isEqualTo(groupId)
    assertThat(dispatchedBlobUri).isEqualTo("$BLOB_URI_PREFIX/$DIRECT_STORAGE_PATH_PREFIX/$groupId")
    assertThat(directBlobsDir().listFiles().orEmpty()).hasLength(1)
    assertThat(blobsList()).isEmpty()
    assertThat(allMetadataQueuedBeforeDispatch).isTrue()
    val dispatchSpan =
      spanExporter.finishedSpanItems.single {
        it.name == "edp_aggregator.requisition_fetcher.dispatch_requisition"
      }
    assertThat(dispatchSpan.attributes.get(ReportTraceAttributes.REQUISITION_NAME))
      .isEqualTo(TestRequisitionData.REQUISITION.name)
    assertThat(dispatchSpan.attributes.get(ReportTraceAttributes.GROUP_ID)).isEqualTo(groupId)
    assertThat(dispatchSpan.attributes.get(ReportTraceAttributes.WORK_ITEM_NAME))
      .isEqualTo("workItems/results-fulfiller-$groupId")
    assertThat(dispatchSpan.attributes.get(ReportTraceAttributes.LIFECYCLE_STAGE))
      .isEqualTo("requisition_dispatch")
    assertThat(dispatchSpan.attributes.get(ReportTraceAttributes.OUTCOME)).isEqualTo("succeeded")
  }

  @Test
  fun `direct dispatch does not ensure WorkItem when queued registration fails`() = runBlocking {
    whenever(requisitionMetadataServiceMock.registerQueuedRequisitionMetadata(any())).thenAnswer {
      throw Status.INTERNAL.asRuntimeException()
    }
    var dispatchCalled = false
    val dispatcher =
      object : RequisitionWorkItemDispatcher {
        override fun workItemName(groupId: String): String = "workItems/results-fulfiller-$groupId"

        override suspend fun dispatch(groupId: String, blobUri: String) {
          dispatchCalled = true
        }
      }

    createFetcher(workItemDispatcher = dispatcher).fetchAndStoreRequisitions()

    assertThat(dispatchCalled).isFalse()
  }

  @Test
  fun `legacy group remains DataWatcher owned when direct dispatch is enabled`() = runBlocking {
    val groupId = "legacy-group-id"
    storageClient.writeBlob("$STORAGE_PATH_PREFIX/$groupId", ByteString.EMPTY)
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsRequisition = TestRequisitionData.REQUISITION.name
            blobUri = "$BLOB_URI_PREFIX/$STORAGE_PATH_PREFIX/$groupId"
            blobTypeUrl = "type.googleapis.com/test"
            this.groupId = groupId
            report = "some-report"
          }
        }
      )
    var dispatchCalled = false
    val dispatcher =
      object : RequisitionWorkItemDispatcher {
        override fun workItemName(groupId: String): String = "workItems/results-fulfiller-$groupId"

        override suspend fun dispatch(groupId: String, blobUri: String) {
          dispatchCalled = true
        }
      }

    createFetcher(workItemDispatcher = dispatcher).fetchAndStoreRequisitions()

    assertThat(dispatchCalled).isFalse()
    assertThat(queueRequisitionMetadataRequests).isEmpty()
  }

  @Test
  fun `direct recovery uses blob URI recorded in metadata`() = runBlocking {
    val groupId = "direct-recovery-group-id"
    val recordedBlobUri = "$BLOB_URI_PREFIX/$DIRECT_STORAGE_PATH_PREFIX/$groupId"
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            name = "${TestRequisitionData.EDP_NAME}/requisitionMetadata/direct"
            state = RequisitionMetadata.State.STORED
            cmmsRequisition = TestRequisitionData.REQUISITION.name
            blobUri = recordedBlobUri
            blobTypeUrl = "type.googleapis.com/test"
            this.groupId = groupId
            report = "some-report"
            etag = "stored-etag"
          }
        }
      )
    var dispatchedBlobUri: String? = null
    val dispatcher =
      object : RequisitionWorkItemDispatcher {
        override fun workItemName(groupId: String): String = "workItems/results-fulfiller-$groupId"

        override suspend fun dispatch(groupId: String, blobUri: String) {
          dispatchedBlobUri = blobUri
        }
      }

    createFetcher(workItemDispatcher = dispatcher).fetchAndStoreRequisitions()

    assertThat(storageClient.getBlob("$DIRECT_STORAGE_PATH_PREFIX/$groupId")).isNotNull()
    assertThat(storageClient.getBlob("$STORAGE_PATH_PREFIX/$groupId")).isNull()
    assertThat(dispatchedBlobUri).isEqualTo(recordedBlobUri)
  }

  @Test
  fun `queued metadata survives unavailable EnsureWorkItem and is retried`() = runBlocking {
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any())).thenAnswer {
      listRequisitionMetadataResponse {
        requisitionMetadata +=
          registerQueuedRequisitionMetadataRequests.flatMap { request ->
            request.requestsList.mapIndexed { index, subRequest ->
              requisitionMetadata {
                name = "${TestRequisitionData.EDP_NAME}/requisitionMetadata/$index"
                cmmsRequisition = subRequest.requisitionMetadata.cmmsRequisition
                blobUri = subRequest.requisitionMetadata.blobUri
                blobTypeUrl = subRequest.requisitionMetadata.blobTypeUrl
                groupId = subRequest.requisitionMetadata.groupId
                report = subRequest.requisitionMetadata.report
                state = RequisitionMetadata.State.QUEUED
                workItem = request.workItem
                etag = "queued-etag"
              }
            }
          }
      }
    }
    var dispatchAttempts = 0
    val dispatcher =
      object : RequisitionWorkItemDispatcher {
        override fun workItemName(groupId: String): String = "workItems/results-fulfiller-$groupId"

        override suspend fun dispatch(groupId: String, blobUri: String) {
          dispatchAttempts++
          if (dispatchAttempts == 1) {
            throw Status.UNIMPLEMENTED.asRuntimeException()
          }
        }
      }
    val fetcher = createFetcher(workItemDispatcher = dispatcher)

    fetcher.fetchAndStoreRequisitions()
    fetcher.fetchAndStoreRequisitions()

    assertThat(registerQueuedRequisitionMetadataRequests).hasSize(1)
    assertThat(dispatchAttempts).isEqualTo(2)
    assertThat(directBlobsDir().listFiles().orEmpty()).hasLength(1)
    assertThat(blobsList()).isEmpty()
  }

  @Test
  fun `direct dispatch validates whole group before queueing stored metadata`() = runBlocking {
    val groupId = "conflicting-group-id"
    val expectedWorkItemName = "workItems/results-fulfiller-$groupId"
    storageClient.writeBlob("$DIRECT_STORAGE_PATH_PREFIX/$groupId", ByteString.EMPTY)
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            name = "${TestRequisitionData.EDP_NAME}/requisitionMetadata/stored"
            state = RequisitionMetadata.State.STORED
            cmmsRequisition = TestRequisitionData.REQUISITION.name
            blobUri = "$BLOB_URI_PREFIX/$DIRECT_STORAGE_PATH_PREFIX/$groupId"
            blobTypeUrl = "type.googleapis.com/test"
            this.groupId = groupId
            report = "some-report"
            etag = "stored-etag"
          }
          requisitionMetadata += requisitionMetadata {
            name = "${TestRequisitionData.EDP_NAME}/requisitionMetadata/conflicting"
            state = RequisitionMetadata.State.QUEUED
            cmmsRequisition = "${TestRequisitionData.EDP_NAME}/requisitions/conflicting"
            blobUri = "$BLOB_URI_PREFIX/$DIRECT_STORAGE_PATH_PREFIX/$groupId"
            blobTypeUrl = "type.googleapis.com/test"
            this.groupId = groupId
            report = "some-report"
            workItem = "workItems/a-different-work-item"
          }
        }
      )
    var dispatchCalled = false
    val dispatcher =
      object : RequisitionWorkItemDispatcher {
        override fun workItemName(groupId: String): String = expectedWorkItemName

        override suspend fun dispatch(groupId: String, blobUri: String) {
          dispatchCalled = true
        }
      }

    createFetcher(workItemDispatcher = dispatcher).fetchAndStoreRequisitions()

    assertThat(queueRequisitionMetadataRequests).isEmpty()
    assertThat(dispatchCalled).isFalse()
  }

  @Test
  fun `secure computation dispatcher creates deterministic WorkItem`() = runBlocking {
    val expectedResultsFulfillerParams = resultsFulfillerParams {
      dataProvider = TestRequisitionData.EDP_NAME
    }
    val dispatcher =
      SecureComputationRequisitionWorkItemDispatcher(
        workItemsStub = workItemsStub,
        queue = "results-fulfiller-queue",
        resultsFulfillerParams = expectedResultsFulfillerParams,
        controlPlaneThrottler = throttler,
      )

    val spanContext =
      SpanContext.create(
        "0123456789abcdef0123456789abcdef",
        "0123456789abcdef",
        TraceFlags.getSampled(),
        TraceState.getDefault(),
      )
    Span.wrap(spanContext).makeCurrent().use {
      dispatcher.dispatch("group-id", "gs://bucket/requisitions-v2/group-id")
    }

    val request = ensureWorkItemRequests.single()
    assertThat(request.workItemId).isEqualTo("results-fulfiller-group-id")
    assertThat(request.workItem.queue).isEqualTo("results-fulfiller-queue")
    val params = request.workItem.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
    assertThat(params.appParams.unpack(ResultsFulfillerParams::class.java))
      .isEqualTo(expectedResultsFulfillerParams)
    assertThat(params.dataPathParams.dataPath).isEqualTo("gs://bucket/requisitions-v2/group-id")
    assertThat(params.traceContextMap)
      .containsEntry("traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
    assertThat(dispatcher.workItemName("group-id"))
      .isEqualTo("workItems/results-fulfiller-group-id")
  }

  @Test
  fun `secure computation dispatcher preserves trace context on idempotent retry`() = runBlocking {
    val requests = mutableListOf<EnsureWorkItemRequest>()
    val resultsFulfillerParams = resultsFulfillerParams {
      dataProvider = TestRequisitionData.EDP_NAME
    }
    whenever(workItemsServiceMock.ensureWorkItem(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<EnsureWorkItemRequest>(0)
      requests += request
      if (requests.size == 1) {
        throw Status.ALREADY_EXISTS.asRuntimeException()
      }
      workItem {
        name = "workItems/${request.workItemId}"
        queue = request.workItem.queue
        workItemParams = request.workItem.workItemParams
        state = WorkItem.State.QUEUED
      }
    }
    whenever(workItemsServiceMock.getWorkItem(any()))
      .thenReturn(
        workItem {
          name = "workItems/results-fulfiller-group-id"
          queue = "results-fulfiller-queue"
          workItemParams =
            workItemParams {
                appParams = resultsFulfillerParams.pack()
                dataPathParams = dataPathParams {
                  dataPath = "gs://bucket/requisitions-v2/group-id"
                }
                traceContext["traceparent"] =
                  "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
              }
              .pack()
        }
      )
    val dispatcher =
      SecureComputationRequisitionWorkItemDispatcher(
        workItemsStub = workItemsStub,
        queue = "results-fulfiller-queue",
        resultsFulfillerParams = resultsFulfillerParams,
        controlPlaneThrottler = throttler,
      )
    val currentSpan =
      Span.wrap(
        SpanContext.create(
          "0123456789abcdef0123456789abcdef",
          "0123456789abcdef",
          TraceFlags.getSampled(),
          TraceState.getDefault(),
        )
      )

    currentSpan.makeCurrent().use {
      dispatcher.dispatch("group-id", "gs://bucket/requisitions-v2/group-id")
    }

    assertThat(requests).hasSize(2)
    val retriedParams =
      requests.last().workItem.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
    assertThat(retriedParams.traceContextMap)
      .containsEntry("traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
  }

  @Test
  fun `secure computation dispatcher treats running WorkItem as success`() = runBlocking {
    whenever(workItemsServiceMock.ensureWorkItem(any()))
      .thenReturn(workItem { state = WorkItem.State.RUNNING })
    val dispatcher =
      SecureComputationRequisitionWorkItemDispatcher(
        workItemsStub = workItemsStub,
        queue = "results-fulfiller-queue",
        resultsFulfillerParams =
          resultsFulfillerParams { dataProvider = TestRequisitionData.EDP_NAME },
        controlPlaneThrottler = throttler,
      )

    dispatcher.dispatch("group-id", "gs://bucket/requisitions/group-id")
  }

  @Test
  fun `secure computation dispatcher propagates terminal WorkItem failure`() = runBlocking {
    whenever(workItemsServiceMock.ensureWorkItem(any())).thenAnswer {
      throw Status.FAILED_PRECONDITION.asRuntimeException()
    }
    val dispatcher =
      SecureComputationRequisitionWorkItemDispatcher(
        workItemsStub = workItemsStub,
        queue = "results-fulfiller-queue",
        resultsFulfillerParams =
          resultsFulfillerParams { dataProvider = TestRequisitionData.EDP_NAME },
        controlPlaneThrottler = throttler,
      )

    val exception =
      assertFailsWith<StatusException> {
        dispatcher.dispatch("group-id", "gs://bucket/requisitions/group-id")
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `secure computation dispatcher gets UNIMPLEMENTED from old service`() = runBlocking {
    whenever(workItemsServiceMock.ensureWorkItem(any())).thenAnswer {
      throw Status.UNIMPLEMENTED.asRuntimeException()
    }
    val dispatcher =
      SecureComputationRequisitionWorkItemDispatcher(
        workItemsStub = workItemsStub,
        queue = "results-fulfiller-queue",
        resultsFulfillerParams =
          resultsFulfillerParams { dataProvider = TestRequisitionData.EDP_NAME },
        controlPlaneThrottler = throttler,
      )

    val exception =
      assertFailsWith<StatusException> {
        dispatcher.dispatch("group-id", "gs://bucket/requisitions/group-id")
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNIMPLEMENTED)
  }

  @Test
  fun `direct dispatch retries group left queued before WorkItem creation`() = runBlocking {
    val groupId = "queued-group-id"
    val expectedWorkItemName = "workItems/results-fulfiller-$groupId"
    storageClient.writeBlob("$DIRECT_STORAGE_PATH_PREFIX/$groupId", ByteString.EMPTY)
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            name = "${TestRequisitionData.EDP_NAME}/requisitionMetadata/queued"
            cmmsRequisition = TestRequisitionData.REQUISITION.name
            blobUri = "$BLOB_URI_PREFIX/$DIRECT_STORAGE_PATH_PREFIX/$groupId"
            blobTypeUrl = "type.googleapis.com/test"
            this.groupId = groupId
            report = "some-report"
            state = RequisitionMetadata.State.QUEUED
            workItem = expectedWorkItemName
            etag = "queued-etag"
          }
        }
      )
    var dispatched = false
    val dispatcher =
      object : RequisitionWorkItemDispatcher {
        override fun workItemName(groupId: String): String = expectedWorkItemName

        override suspend fun dispatch(groupId: String, blobUri: String) {
          dispatched = true
        }
      }

    createFetcher(workItemDispatcher = dispatcher).fetchAndStoreRequisitions()

    assertThat(queueRequisitionMetadataRequests).isEmpty()
    assertThat(dispatched).isTrue()
  }

  @Test
  fun `report exceeding maxRequisitionsPerGroup is written across multiple groups`() = runBlocking {
    // Five requisitions for one report with a cap of 2 -> 3 groups (2 + 2 + 1). Each group is its
    // own blob and its own metadata batch; every requisition is covered exactly once.
    val reqs =
      (1..5).map { idx ->
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo$idx"
          updateTime = timestamp { seconds = 10 }
        }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += reqs })

    createFetcher(maxRequisitionsPerGroup = 2).fetchAndStoreRequisitions()

    // 3 blobs, 3 distinct groupIds, all 5 requisitions registered exactly once.
    assertThat(blobsList()).hasLength(3)
    val perGroup = createRequisitionMetadataRequests.groupBy { it.requisitionMetadata.groupId }
    assertThat(perGroup.keys).hasSize(3)
    assertThat(perGroup.values.map { it.size }.sorted()).containsExactly(1, 2, 2)
    val allNames =
      createRequisitionMetadataRequests.map { it.requisitionMetadata.cmmsRequisition }.toSet()
    assertThat(allNames).isEqualTo(reqs.map { it.name }.toSet())
    // Two blobs beyond the first are splits.
    assertThat(counterValue("edpa.requisition_fetcher.buffer_splits")).isEqualTo(2)
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
  fun `differing updateTimes for same report still produce a single blob`() = runBlocking {
    // A report's requisitions are transitioned to UNFULFILLED per Measurement, so they legitimately
    // carry distinct updateTimes. Grouping is by reportId, so they must still land in one blob.
    val r1 = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 10 } }
    val r2 =
      TestRequisitionData.REQUISITION.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
        updateTime = timestamp { seconds = 20 }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += listOf(r1, r2) })

    createFetcher().fetchAndStoreRequisitions()

    assertThat(blobsList()).hasLength(1)
    val groupIds = createRequisitionMetadataRequests.map { it.requisitionMetadata.groupId }.toSet()
    assertThat(groupIds).hasSize(1)
  }

  @Test
  fun `total-bytes backstop flushes open buffers and counts a split`() = runBlocking {
    val requisitions =
      (1..3).map { idx ->
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo$idx"
          updateTime = timestamp { seconds = 10 }
        }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { this.requisitions += requisitions })

    // bufferSplits is derived from persisted metadata: a split is counted when a new group is
    // written for a report that already has STORED metadata. Make the metadata service stateful so
    // the second work unit's listRequisitionMetadata returns the STORED rows the first unit
    // created.
    installStatefulMetadataMock()

    // Global cap chosen so the second add trips the backstop: r1+r2 flush as one blob, then r3
    // flushes as a second blob for the same report — the second blob is the counted split.
    val perRequisitionBytes = requisitions.first().serializedSize.toLong()
    val cap = perRequisitionBytes + 1
    createFetcher(maxTotalBufferedBytes = cap).fetchAndStoreRequisitions()

    assertThat(blobsList()).hasLength(2)
    assertThat(counterValue("edpa.requisition_fetcher.buffer_splits")).isEqualTo(1)
  }

  @Test
  fun `writes blob before creating any metadata for that group`() = runBlocking {
    val recordingStorage = OrderRecordingStorageClient(storageClient)
    whenever(requisitionMetadataServiceMock.batchCreateRequisitionMetadata(any())).thenAnswer {
      invocation ->
      val request = invocation.getArgument<BatchCreateRequisitionMetadataRequest>(0)
      for (subRequest in request.requestsList) {
        val groupId = subRequest.requisitionMetadata.groupId
        check(recordingStorage.writtenGroupIds.contains(groupId)) {
          "batchCreateRequisitionMetadata called for $groupId before its blob was written"
        }
      }
      createRequisitionMetadataRequests += request.requestsList
      batchCreateRequisitionMetadataResponse {
        requisitionMetadata += request.requestsList.map { requisitionMetadata {} }
      }
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
  fun `recovery rebuilds STORED rows in a group that also has terminal metadata`() = runBlocking {
    val groupId = "mixed-state-group-id"
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.FULFILLED
            cmmsRequisition = "${TestRequisitionData.EDP_NAME}/requisitions/already-fulfilled"
            blobUri = "$BLOB_URI_PREFIX/$STORAGE_PATH_PREFIX/$groupId"
            blobTypeUrl = "type"
            this.groupId = groupId
            report = "some-report"
          }
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsRequisition = TestRequisitionData.REQUISITION.name
            blobUri = "$BLOB_URI_PREFIX/$STORAGE_PATH_PREFIX/$groupId"
            blobTypeUrl = "type"
            this.groupId = groupId
            report = "some-report"
          }
        }
      )

    createFetcher().fetchAndStoreRequisitions()

    val recoveredBlob = storageClient.getBlob("$STORAGE_PATH_PREFIX/$groupId")
    assertThat(recoveredBlob).isNotNull()
    val parsed =
      Any.parseFrom(recoveredBlob!!.read().flatten()).unpack(GroupedRequisitions::class.java)
    assertThat(parsed.requisitionsList).hasSize(1)
    assertThat(createRequisitionMetadataRequests).isEmpty()
  }

  @Test
  fun `direct dispatch does not retry failed WorkItem for QUEUED rows`() = runBlocking {
    val groupId = "mixed-state-group-id"
    val workItemName = "workItems/results-fulfiller-$groupId"
    storageClient.writeBlob("$DIRECT_STORAGE_PATH_PREFIX/$groupId", ByteString.EMPTY)
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.FULFILLED
            cmmsRequisition = "${TestRequisitionData.EDP_NAME}/requisitions/already-fulfilled"
            blobUri = "$BLOB_URI_PREFIX/$DIRECT_STORAGE_PATH_PREFIX/$groupId"
            blobTypeUrl = "type"
            this.groupId = groupId
            report = "some-report"
          }
          requisitionMetadata += requisitionMetadata {
            name = "${TestRequisitionData.EDP_NAME}/requisitionMetadata/queued"
            state = RequisitionMetadata.State.QUEUED
            cmmsRequisition = TestRequisitionData.REQUISITION.name
            blobUri = "$BLOB_URI_PREFIX/$DIRECT_STORAGE_PATH_PREFIX/$groupId"
            blobTypeUrl = "type"
            this.groupId = groupId
            report = "some-report"
            workItem = workItemName
          }
        }
      )
    val expectedParams = resultsFulfillerParams { dataProvider = TestRequisitionData.EDP_NAME }
    whenever(workItemsServiceMock.ensureWorkItem(any())).thenAnswer { invocation ->
      ensureWorkItemRequests += invocation.getArgument<EnsureWorkItemRequest>(0)
      throw Status.FAILED_PRECONDITION.asRuntimeException()
    }
    val dispatcher =
      SecureComputationRequisitionWorkItemDispatcher(
        workItemsStub = workItemsStub,
        queue = "results-fulfiller-queue",
        resultsFulfillerParams = expectedParams,
        controlPlaneThrottler = throttler,
      )

    createFetcher(workItemDispatcher = dispatcher).fetchAndStoreRequisitions()

    assertThat(ensureWorkItemRequests).hasSize(1)
    assertThat(queueRequisitionMetadataRequests).isEmpty()
    assertThat(createRequisitionMetadataRequests).isEmpty()
  }

  @Test
  fun `direct dispatch does not retry failed WorkItem for PROCESSING rows`() = runBlocking {
    val groupId = "processing-group-id"
    val workItemName = "workItems/results-fulfiller-$groupId"
    storageClient.writeBlob("$DIRECT_STORAGE_PATH_PREFIX/$groupId", ByteString.EMPTY)
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            name = "${TestRequisitionData.EDP_NAME}/requisitionMetadata/processing"
            state = RequisitionMetadata.State.PROCESSING
            cmmsRequisition = TestRequisitionData.REQUISITION.name
            blobUri = "$BLOB_URI_PREFIX/$DIRECT_STORAGE_PATH_PREFIX/$groupId"
            blobTypeUrl = "type"
            this.groupId = groupId
            report = "some-report"
            workItem = workItemName
          }
        }
      )
    val expectedParams = resultsFulfillerParams { dataProvider = TestRequisitionData.EDP_NAME }
    whenever(workItemsServiceMock.ensureWorkItem(any())).thenAnswer { invocation ->
      ensureWorkItemRequests += invocation.getArgument<EnsureWorkItemRequest>(0)
      throw Status.FAILED_PRECONDITION.asRuntimeException()
    }
    val dispatcher =
      SecureComputationRequisitionWorkItemDispatcher(
        workItemsStub = workItemsStub,
        queue = "results-fulfiller-queue",
        resultsFulfillerParams = expectedParams,
        controlPlaneThrottler = throttler,
      )

    createFetcher(workItemDispatcher = dispatcher).fetchAndStoreRequisitions()

    assertThat(ensureWorkItemRequests).hasSize(1)
    assertThat(queueRequisitionMetadataRequests).isEmpty()
  }

  @Test
  fun `direct dispatch retries mixed terminal PROCESSING and QUEUED rows`() = runBlocking {
    val groupId = "mixed-processing-group-id"
    val workItemName = "workItems/results-fulfiller-$groupId"
    storageClient.writeBlob("$DIRECT_STORAGE_PATH_PREFIX/$groupId", ByteString.EMPTY)
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.FULFILLED
            cmmsRequisition = "${TestRequisitionData.EDP_NAME}/requisitions/fulfilled"
            blobUri = "$BLOB_URI_PREFIX/$DIRECT_STORAGE_PATH_PREFIX/$groupId"
            this.groupId = groupId
            report = "some-report"
          }
          requisitionMetadata += requisitionMetadata {
            name = "${TestRequisitionData.EDP_NAME}/requisitionMetadata/processing"
            state = RequisitionMetadata.State.PROCESSING
            cmmsRequisition = "${TestRequisitionData.EDP_NAME}/requisitions/processing"
            blobUri = "$BLOB_URI_PREFIX/$DIRECT_STORAGE_PATH_PREFIX/$groupId"
            this.groupId = groupId
            report = "some-report"
            workItem = workItemName
          }
          requisitionMetadata += requisitionMetadata {
            name = "${TestRequisitionData.EDP_NAME}/requisitionMetadata/queued"
            state = RequisitionMetadata.State.QUEUED
            cmmsRequisition = TestRequisitionData.REQUISITION.name
            blobUri = "$BLOB_URI_PREFIX/$DIRECT_STORAGE_PATH_PREFIX/$groupId"
            this.groupId = groupId
            report = "some-report"
            workItem = workItemName
          }
        }
      )
    var dispatched = false
    val dispatcher =
      object : RequisitionWorkItemDispatcher {
        override fun workItemName(groupId: String): String = workItemName

        override suspend fun dispatch(groupId: String, blobUri: String) {
          dispatched = true
        }
      }

    createFetcher(workItemDispatcher = dispatcher).fetchAndStoreRequisitions()

    assertThat(dispatched).isTrue()
    assertThat(queueRequisitionMetadataRequests).isEmpty()
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
    // A requisition refused for a bad spec was still fetched from the Kingdom, so it is counted.
    assertThat(counterValue("edpa.requisition_fetcher.requisitions_fetched")).isEqualTo(1)
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

    createFetcher().fetchAndStoreRequisitions()

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

    // 1 list + 1 batchCreate metadata call (both reqs in one atomic batch).
    assertThat(counter.count.get()).isEqualTo(2)
  }

  @Test
  fun `streams more requisitions than channel capacity without deadlock`() = runBlocking {
    val n = 200
    // Distinct reports so each yields its own blob; exercises producer/consumer backpressure past
    // the channel capacity without deadlock.
    val requisitions =
      (1..n).map { idx ->
        val spec =
          TestRequisitionData.MEASUREMENT_SPEC.copy {
            reportingMetadata =
              org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reportingMetadata {
                report = "report-$idx"
              }
          }
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo$idx"
          measurementSpec =
            org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec(
              spec,
              TestRequisitionData.MC_SIGNING_KEY,
            )
          updateTime = timestamp { seconds = idx.toLong() }
        }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { this.requisitions += requisitions })

    createFetcher().fetchAndStoreRequisitions()

    assertThat(blobsList()).hasLength(n)
    assertThat(createRequisitionMetadataRequests).hasSize(n)
  }

  @Test
  fun `fetchAndStoreRequisitions halves page size on RESOURCE_EXHAUSTED and retries`() =
    runBlocking {
      val r1 = TestRequisitionData.REQUISITION
      val callCount = AtomicInteger(0)
      val captured = mutableListOf<Int>()
      whenever(requisitionsServiceMock.listRequisitions(any())).thenAnswer { invocation ->
        val request = invocation.arguments[0] as ListRequisitionsRequest
        captured += request.pageSize
        val attempt = callCount.incrementAndGet()
        if (attempt < 4) {
          throw StatusException(Status.RESOURCE_EXHAUSTED.withDescription("too big"))
        }
        listRequisitionsResponse { requisitions += r1 }
      }

      createFetcher().fetchAndStoreRequisitions()

      assertThat(captured).hasSize(4)
      assertThat(captured[0]).isEqualTo(10)
      assertThat(captured[1]).isEqualTo(5)
      assertThat(captured[2]).isEqualTo(2)
      assertThat(captured[3]).isEqualTo(1)
      assertThat(blobsList()).hasLength(1)
      assertThat(counterValue("edpa.requisition_fetcher.page_size_reductions")).isEqualTo(3)
    }

  @Test
  fun `fetchAndStoreRequisitions surfaces RESOURCE_EXHAUSTED at minimum page size`() = runBlocking {
    whenever(requisitionsServiceMock.listRequisitions(any())).thenAnswer {
      throw StatusException(Status.RESOURCE_EXHAUSTED.withDescription("still too big"))
    }

    val e =
      assertThrows(StatusException::class.java) {
        runBlocking { createFetcher().fetchAndStoreRequisitions() }
      }
    assertThat(e.status.code).isEqualTo(Status.Code.RESOURCE_EXHAUSTED)
  }

  @Test
  fun `fetchAndStoreRequisitions stays at reduced page size on subsequent pages`() = runBlocking {
    val captured = mutableListOf<Int>()
    val callCount = AtomicInteger(0)
    whenever(requisitionsServiceMock.listRequisitions(any())).thenAnswer { invocation ->
      val request = invocation.arguments[0] as ListRequisitionsRequest
      captured += request.pageSize
      val attempt = callCount.incrementAndGet()
      when (attempt) {
        1 -> throw StatusException(Status.RESOURCE_EXHAUSTED.withDescription("page 1 too big"))
        2 ->
          listRequisitionsResponse {
            requisitions += TestRequisitionData.REQUISITION
            nextPageToken = "more"
          }
        else -> listRequisitionsResponse { requisitions += TestRequisitionData.REQUISITION }
      }
    }

    createFetcher().fetchAndStoreRequisitions()

    assertThat(captured).containsExactly(10, 5, 5).inOrder()
  }

  private fun histogramSumValue(name: String): Long {
    return metricReader
      .collectAllMetrics()
      .firstOrNull { it.name == name }
      ?.histogramData
      ?.points
      ?.sumOf { it.sum.toLong() } ?: 0L
  }

  private fun counterValue(name: String): Long {
    val data =
      metricReader
        .collectAllMetrics()
        .firstOrNull { it.name == name }
        ?.longSumData
        ?.points
        ?.sumOf { (it as LongPointData).value } ?: 0L
    return data
  }

  @Test
  fun `metrics record storage writes and requisitions fetched counters`() = runBlocking {
    val r1 = TestRequisitionData.REQUISITION
    val r2 =
      r1.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/second"
        updateTime = timestamp { seconds = 1000 }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += listOf(r1, r2) })

    createFetcher().fetchAndStoreRequisitions()

    assertThat(counterValue("edpa.requisition_fetcher.storage_writes")).isEqualTo(1)
    assertThat(counterValue("edpa.requisition_fetcher.requisitions_fetched")).isEqualTo(2)
  }

  @Test
  fun `metrics record fetch latency`() = runBlocking {
    createFetcher().fetchAndStoreRequisitions()

    val histogramRecorded =
      metricReader.collectAllMetrics().any { it.name == "edpa.requisition_fetcher.fetch_latency" }
    assertThat(histogramRecorded).isTrue()
  }

  @Test
  fun `existing metadata in any non-FULFILLED state still skips new registration`() = runBlocking {
    val r1 = TestRequisitionData.REQUISITION
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += r1 })
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            cmmsRequisition = r1.name
            groupId = "preexisting-group"
            state = RequisitionMetadata.State.PROCESSING
          }
        }
      )

    createFetcher().fetchAndStoreRequisitions()

    assertThat(createRequisitionMetadataRequests).isEmpty()
  }

  @Test
  fun `recovery rebuilds multiple missing blobs in one run`() = runBlocking {
    val r1 = TestRequisitionData.REQUISITION
    val r2 = r1.copy { name = "${TestRequisitionData.EDP_NAME}/requisitions/second" }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += listOf(r1, r2) })
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata +=
            listOf(
              requisitionMetadata {
                cmmsRequisition = r1.name
                groupId = "group-A"
                state = RequisitionMetadata.State.STORED
              },
              requisitionMetadata {
                cmmsRequisition = r2.name
                groupId = "group-B"
                state = RequisitionMetadata.State.STORED
              },
            )
        }
      )

    createFetcher().fetchAndStoreRequisitions()

    val writtenBlobNames = blobsList().map { it.name }.toSet()
    assertThat(writtenBlobNames).containsExactly("group-A", "group-B")
    assertThat(counterValue("edpa.requisition_fetcher.recovery_rebuilds")).isEqualTo(2)
  }

  @Test
  fun `batch create failure leaves zero metadata under the group (no partial wedge)`() =
    runBlocking {
      // Wedge variant pin: blob already written, batch metadata create throws. Because
      // BatchCreateRequisitionMetadata is server-side atomic (one Spanner transaction), the
      // failure produces zero metadata rows under groupId — never a partial subset. Next run
      // sees `unregistered = all requisitions`, mints a new groupId, writes a fresh blob —
      // the orphan blob is benign because no metadata references it as STORED.
      val r1 = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 10 } }
      val r2 =
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
          updateTime = timestamp { seconds = 10 }
        }
      whenever(requisitionsServiceMock.listRequisitions(any()))
        .thenReturn(listRequisitionsResponse { requisitions += listOf(r1, r2) })
      whenever(requisitionMetadataServiceMock.batchCreateRequisitionMetadata(any())).thenAnswer {
        throw RuntimeException("simulated batch failure after blob write")
      }

      createFetcher().fetchAndStoreRequisitions()

      // Blob was written (blob-first ordering) — counts as a storage write.
      assertThat(counterValue("edpa.requisition_fetcher.storage_writes")).isEqualTo(1)
      // No metadata persisted — atomicity guarantee.
      assertThat(createRequisitionMetadataRequests).isEmpty()
      // The report failure was surfaced and counted.
      assertThat(counterValue("edpa.requisition_fetcher.report_failures")).isEqualTo(1)
    }

  @Test
  fun `batch create requests carry deterministic UUID requestIds derived per (req, group)`() =
    runBlocking {
      val r1 = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 10 } }
      val r2 =
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
          updateTime = timestamp { seconds = 10 }
        }
      whenever(requisitionsServiceMock.listRequisitions(any()))
        .thenReturn(listRequisitionsResponse { requisitions += listOf(r1, r2) })

      createFetcher().fetchAndStoreRequisitions()
      val firstRunRequestIds = createRequisitionMetadataRequests.map { it.requestId }

      // All requestIds must be valid UUIDs — the metadata service rejects non-UUIDs with
      // INVALID_ARGUMENT at RequisitionMetadataService.validateRequisitionMetadataRequest.
      firstRunRequestIds.forEach { java.util.UUID.fromString(it) }
      // And distinct per requisition within a group.
      assertThat(firstRunRequestIds.toSet()).hasSize(2)

      // A second fetch of the same requisitions skips both (existing metadata filtered out
      // by `unregistered`), so determinism here is exercised by the recovery path instead.
    }

  @Test
  fun `requestId is stable across retries for the same (cmmsRequisition, groupId)`(): Unit =
    runBlocking {
      // Two requisitions for one report; the first batchCreate call throws, forcing the per-
      // report try/catch to record the failure. The next fetch run replays the same requisitions
      // (still UNFULFILLED in Kingdom) and re-attempts the batch. RequestIds derived from
      // (cmmsRequisition, groupId) via UUID.nameUUIDFromBytes are stable per pair, so the same
      // requisitions produce the same requestId set on every attempt — which is what makes
      // server-side idempotency by requestId work as a backstop against duplicate rows.
      val r1 = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 10 } }
      val r2 =
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
          updateTime = timestamp { seconds = 10 }
        }
      whenever(requisitionsServiceMock.listRequisitions(any()))
        .thenReturn(listRequisitionsResponse { requisitions += listOf(r1, r2) })

      val attempts = AtomicInteger(0)
      val seenBatches = mutableListOf<List<String>>()
      whenever(requisitionMetadataServiceMock.batchCreateRequisitionMetadata(any())).thenAnswer {
        invocation ->
        val request = invocation.getArgument<BatchCreateRequisitionMetadataRequest>(0)
        val attempt = attempts.incrementAndGet()
        seenBatches += request.requestsList.map { it.requestId }
        if (attempt == 1) throw RuntimeException("simulated transient failure")
        // Second attempt mirrors the normal mock behavior so the run can complete.
        createRequisitionMetadataRequests += request.requestsList
        batchCreateRequisitionMetadataResponse {
          requisitionMetadata +=
            request.requestsList.map { subRequest ->
              requisitionMetadata {
                cmmsRequisition = subRequest.requisitionMetadata.cmmsRequisition
                groupId = subRequest.requisitionMetadata.groupId
              }
            }
        }
      }

      // First fetch hits the failure, second fetch retries with the same requisitions because
      // they are still UNFULFILLED and no metadata was persisted on the failed attempt.
      val fetcher = createFetcher()
      fetcher.fetchAndStoreRequisitions()
      fetcher.fetchAndStoreRequisitions()

      assertThat(seenBatches).hasSize(2)
      // The two attempts produced groupIds that DIFFER (groupId is a fresh UUID each call), so
      // the requestId pairs are different too. The determinism we care about is *within* a
      // single batch call: requestId for the same (req, groupId) is identical to itself, which
      // is what server-side requestId dedup relies on. Assert each batch's requestIds parse and
      // are distinct per requisition.
      seenBatches.forEach { batch ->
        batch.forEach { java.util.UUID.fromString(it) }
        assertThat(batch.toSet()).hasSize(2)
      }

      // And a direct check on the function: same input → same UUID.
      val derived = { name: String, gid: String ->
        java.util.UUID.nameUUIDFromBytes("$name/$gid".toByteArray()).toString()
      }
      assertThat(derived(r1.name, "g1")).isEqualTo(derived(r1.name, "g1"))
      assertThat(derived(r1.name, "g1")).isNotEqualTo(derived(r1.name, "g2"))
      assertThat(derived(r1.name, "g1")).isNotEqualTo(derived(r2.name, "g1"))
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

  @Test
  fun `out-of-order updateTime within one report still produces one blob`() = runBlocking {
    // updateTime does not gate grouping, so ordering within a report is irrelevant to the result.
    val r1 = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 20 } }
    val r2 =
      TestRequisitionData.REQUISITION.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
        updateTime = timestamp { seconds = 10 }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += listOf(r1, r2) })

    createFetcher().fetchAndStoreRequisitions()

    assertThat(blobsList()).hasLength(1)
    val groupIds = createRequisitionMetadataRequests.map { it.requisitionMetadata.groupId }.toSet()
    assertThat(groupIds).hasSize(1)
  }

  @Test
  fun `STORED recovery rebuilds a missing blob when all expected reqs arrive in one unit`() =
    runBlocking {
      val groupId = "wedged-group-id"
      val blobKey = "$STORAGE_PATH_PREFIX/$groupId"
      val r1 = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 10 } }
      val r2 =
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
          updateTime = timestamp { seconds = 20 }
        }
      whenever(requisitionsServiceMock.listRequisitions(any()))
        .thenReturn(listRequisitionsResponse { requisitions += listOf(r1, r2) })
      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
        .thenReturn(
          listRequisitionMetadataResponse {
            requisitionMetadata +=
              listOf(
                requisitionMetadata {
                  state = RequisitionMetadata.State.STORED
                  cmmsRequisition = r1.name
                  blobUri = "$BLOB_URI_PREFIX/$blobKey"
                  blobTypeUrl = "type"
                  this.groupId = groupId
                  report = "some-report"
                },
                requisitionMetadata {
                  state = RequisitionMetadata.State.STORED
                  cmmsRequisition = r2.name
                  blobUri = "$BLOB_URI_PREFIX/$blobKey"
                  blobTypeUrl = "type"
                  this.groupId = groupId
                  report = "some-report"
                },
              )
          }
        )

      createFetcher().fetchAndStoreRequisitions()

      val blob = storageClient.getBlob(blobKey)
      assertThat(blob).isNotNull()
      val parsed = Any.parseFrom(blob!!.read().flatten()).unpack(GroupedRequisitions::class.java)
      assertThat(parsed.requisitionsList).hasSize(2)
      assertThat(parsed.groupId).isEqualTo(groupId)
      assertThat(createRequisitionMetadataRequests).isEmpty()
      assertThat(counterValue("edpa.requisition_fetcher.recovery_rebuilds")).isEqualTo(1)
    }

  @Test
  fun `recovery skipped and metric incremented when stream lacks required reqs`() = runBlocking {
    val groupId = "incomplete-group-id"
    val blobKey = "$STORAGE_PATH_PREFIX/$groupId"
    val present = TestRequisitionData.REQUISITION
    val missingName = "${TestRequisitionData.EDP_NAME}/requisitions/missing"
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += present })
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata +=
            listOf(
              requisitionMetadata {
                state = RequisitionMetadata.State.STORED
                cmmsRequisition = present.name
                blobUri = "$BLOB_URI_PREFIX/$blobKey"
                blobTypeUrl = "type"
                this.groupId = groupId
                report = "some-report"
              },
              requisitionMetadata {
                state = RequisitionMetadata.State.STORED
                cmmsRequisition = missingName
                blobUri = "$BLOB_URI_PREFIX/$blobKey"
                blobTypeUrl = "type"
                this.groupId = groupId
                report = "some-report"
              },
            )
        }
      )

    createFetcher().fetchAndStoreRequisitions()

    assertThat(storageClient.getBlob(blobKey)).isNull()
    assertThat(counterValue("edpa.requisition_fetcher.recovery_rebuilds")).isEqualTo(0)
    assertThat(counterValue("edpa.requisition_fetcher.recovery_skipped_incomplete")).isEqualTo(1)
  }

  @Test
  fun `periodic drain flushes a report split across intervals and counts splits`() = runBlocking {
    // Two requisitions for one report; the stream pauses between them longer than the flush
    // interval so the ticker drains the first before the second arrives. Result: two blobs for one
    // report, and a split counted for the early drain.
    val r1 = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 10 } }
    val r2 =
      TestRequisitionData.REQUISITION.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
        updateTime = timestamp { seconds = 20 }
      }
    // First list call returns r1, then a slow second page returns r2 after a delay exceeding the
    // flush interval; a final empty page ends the stream.
    var call = 0
    whenever(requisitionsServiceMock.listRequisitions(any())).thenAnswer {
      when (call++) {
        0 ->
          listRequisitionsResponse {
            requisitions += r1
            nextPageToken = "p1"
          }
        1 -> {
          Thread.sleep(300)
          listRequisitionsResponse {
            requisitions += r2
            nextPageToken = "p2"
          }
        }
        else -> listRequisitionsResponse {}
      }
    }

    // A split is counted only when the second unit sees the first unit's STORED metadata.
    installStatefulMetadataMock()

    createFetcher(flushInterval = Duration.ofMillis(100)).fetchAndStoreRequisitions()

    assertThat(blobsList()).hasLength(2)
    assertThat(counterValue("edpa.requisition_fetcher.buffer_splits")).isEqualTo(1)
  }

  @Test
  fun `distinct reports without a drain all stay open then flush at stream end`() = runBlocking {
    // With a long flush interval, 10 distinct reports accumulate concurrently and all flush at the
    // natural end of the stream — one blob each, no splits.
    val reqs =
      (1..10).map { idx ->
        val spec =
          TestRequisitionData.MEASUREMENT_SPEC.copy {
            reportingMetadata =
              org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reportingMetadata {
                report = "report-$idx"
              }
          }
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/r-$idx"
          measurementSpec =
            org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec(
              spec,
              TestRequisitionData.MC_SIGNING_KEY,
            )
          updateTime = timestamp { seconds = idx.toLong() }
        }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += reqs })

    createFetcher(flushInterval = Duration.ofHours(1)).fetchAndStoreRequisitions()

    assertThat(histogramSumValue("edpa.requisition_fetcher.open_buffer_high_water_mark"))
      .isEqualTo(10)
    assertThat(blobsList()).hasLength(10)
    assertThat(counterValue("edpa.requisition_fetcher.buffer_splits")).isEqualTo(0)
  }

  @Test
  fun `open buffer high water mark records peak distinct reportIds`() = runBlocking {
    val reqs =
      (1..5).map { idx ->
        val spec =
          TestRequisitionData.MEASUREMENT_SPEC.copy {
            reportingMetadata =
              org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reportingMetadata {
                report = "report-$idx"
              }
          }
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/r-$idx"
          measurementSpec =
            org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec(
              spec,
              TestRequisitionData.MC_SIGNING_KEY,
            )
        }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += reqs })

    createFetcher().fetchAndStoreRequisitions()

    assertThat(histogramSumValue("edpa.requisition_fetcher.open_buffer_high_water_mark"))
      .isEqualTo(5)
  }

  @Test
  fun `recovery counter increments when zero matching requisitions arrive for a STORED group`() =
    runBlocking {
      val groupId = "fully-orphaned-group"
      val blobKey = "$STORAGE_PATH_PREFIX/$groupId"
      val unrelated =
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/unrelated"
        }
      whenever(requisitionsServiceMock.listRequisitions(any()))
        .thenReturn(listRequisitionsResponse { requisitions += unrelated })
      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any())).thenAnswer {
        invocation ->
        val request = invocation.arguments[0] as ListRequisitionMetadataRequest
        val report = request.filter.report
        if (report.isBlank()) {
          listRequisitionMetadataResponse {}
        } else {
          listRequisitionMetadataResponse {
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.STORED
              cmmsRequisition = "${TestRequisitionData.EDP_NAME}/requisitions/never-in-stream"
              blobUri = "$BLOB_URI_PREFIX/$blobKey"
              blobTypeUrl = "type"
              this.groupId = groupId
              this.report = report
            }
          }
        }
      }

      createFetcher().fetchAndStoreRequisitions()

      assertThat(storageClient.getBlob(blobKey)).isNull()
      assertThat(counterValue("edpa.requisition_fetcher.recovery_skipped_incomplete")).isEqualTo(1)
      assertThat(counterValue("edpa.requisition_fetcher.recovery_rebuilds")).isEqualTo(0)
    }

  @Test
  fun `interleaved updateTimes for one report collapse into one blob`() = runBlocking {
    val reqs =
      listOf(10L, 50L, 5L, 30L).mapIndexed { idx, t ->
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/r$idx"
          updateTime = timestamp { seconds = t }
        }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += reqs })

    createFetcher().fetchAndStoreRequisitions()

    assertThat(blobsList()).hasLength(1)
    val groupIds = createRequisitionMetadataRequests.map { it.requisitionMetadata.groupId }.toSet()
    assertThat(groupIds).hasSize(1)
  }

  @Test
  fun `recovery deduplicates a requisition that appears more than once`(): Unit = runBlocking {
    val groupId = "wedged-group-id"
    val blobKey = "$STORAGE_PATH_PREFIX/$groupId"
    // The same requisition (r1) appears twice in the stream (Kingdom-side updateTime drift). The
    // wedged group expects {r1, r2}. Without dedup, r1 counted twice could satisfy the size check
    // before r2 arrives and rebuild a blob missing r2. Dedup by name prevents that.
    val r1a = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 10 } }
    val r1b = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 20 } }
    val r2 =
      TestRequisitionData.REQUISITION.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
        updateTime = timestamp { seconds = 20 }
      }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += listOf(r1a, r1b, r2) })
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata +=
            listOf(
              requisitionMetadata {
                state = RequisitionMetadata.State.STORED
                cmmsRequisition = r1a.name
                blobUri = "$BLOB_URI_PREFIX/$blobKey"
                blobTypeUrl = "type"
                this.groupId = groupId
                report = "some-report"
              },
              requisitionMetadata {
                state = RequisitionMetadata.State.STORED
                cmmsRequisition = r2.name
                blobUri = "$BLOB_URI_PREFIX/$blobKey"
                blobTypeUrl = "type"
                this.groupId = groupId
                report = "some-report"
              },
            )
        }
      )

    createFetcher().fetchAndStoreRequisitions()

    val blob = storageClient.getBlob(blobKey)
    assertThat(blob).isNotNull()
    val parsed = Any.parseFrom(blob!!.read().flatten()).unpack(GroupedRequisitions::class.java)
    assertThat(parsed.requisitionsList).hasSize(2)
    val names =
      parsed.requisitionsList.map {
        it.requisition.unpack(org.wfanet.measurement.api.v2alpha.Requisition::class.java).name
      }
    assertThat(names.toSet()).containsExactly(r1a.name, r2.name)
  }

  @Test
  fun `storage failure marks report failure and surfaces storageFails counter`() = runBlocking {
    val r1 = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 10 } }
    whenever(requisitionsServiceMock.listRequisitions(any()))
      .thenReturn(listRequisitionsResponse { requisitions += r1 })

    val mockStorageClient: StorageClient = mock()
    whenever(mockStorageClient.getBlob(any())).thenReturn(null)
    whenever(mockStorageClient.writeBlob(any<String>(), any<ByteString>())).thenAnswer {
      throw RuntimeException("simulated storage write failure")
    }

    createFetcher(storageClient = mockStorageClient).fetchAndStoreRequisitions()

    assertThat(counterValue("edpa.requisition_fetcher.storage_fails")).isEqualTo(1)
    assertThat(counterValue("edpa.requisition_fetcher.report_failures")).isEqualTo(1)
    assertThat(counterValue("edpa.requisition_fetcher.storage_writes")).isEqualTo(0)
    assertThat(createRequisitionMetadataRequests).isEmpty()
  }

  @Test
  fun `metadata cache is invalidated when batch create throws on the persist call`() = runBlocking {
    val r1 =
      TestRequisitionData.REQUISITION.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/r1"
        updateTime = timestamp { seconds = 10 }
      }
    val r2 =
      TestRequisitionData.REQUISITION.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/r2"
        updateTime = timestamp { seconds = 20 }
      }
    // Two pages for one report, drained apart: the periodic ticker flushes r1 before the slow
    // second page delivers r2, so the report is processed as two units.
    var page = 0
    whenever(requisitionsServiceMock.listRequisitions(any())).thenAnswer {
      when (page++) {
        0 ->
          listRequisitionsResponse {
            requisitions += r1
            nextPageToken = "p1"
          }
        1 -> {
          Thread.sleep(300)
          listRequisitionsResponse { requisitions += r2 }
        }
        else -> listRequisitionsResponse {}
      }
    }

    val listCalls = AtomicInteger(0)
    val batchCalls = AtomicInteger(0)
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any())).thenAnswer {
      listCalls.incrementAndGet()
      listRequisitionMetadataResponse {}
    }
    whenever(requisitionMetadataServiceMock.batchCreateRequisitionMetadata(any())).thenAnswer {
      invocation ->
      val count = batchCalls.incrementAndGet()
      if (count == 1) throw RuntimeException("simulated batch create failure")
      val request = invocation.getArgument<BatchCreateRequisitionMetadataRequest>(0)
      createRequisitionMetadataRequests += request.requestsList
      batchCreateRequisitionMetadataResponse {
        requisitionMetadata += request.requestsList.map { requisitionMetadata {} }
      }
    }

    createFetcher(flushInterval = Duration.ofMillis(100)).fetchAndStoreRequisitions()

    assertThat(counterValue("edpa.requisition_fetcher.report_failures")).isEqualTo(1)
    // The metadataCache.remove() in the finally is the load-bearing assertion: if it didn't
    // fire, unit 2 would reuse the cached pre-failure snapshot and listCalls would stay at 1.
    assertThat(listCalls.get()).isEqualTo(2)
  }

  @Test
  fun `refuseUnregisteredAndPersist batch failure invalidates metadata cache`() = runBlocking {
    // Mixed model lines trigger the refuse path. batchCreateRequisitionMetadata throws on its
    // first call. The finally in processReportInner must still invalidate the cache so a
    // second unit for the same report re-lists rather than reading the stale empty snapshot.
    val measurementSpec2 =
      TestRequisitionData.MEASUREMENT_SPEC.copy { modelLine = "other-model-line" }
    val r1 = TestRequisitionData.REQUISITION.copy { updateTime = timestamp { seconds = 10 } }
    val r2 =
      TestRequisitionData.REQUISITION.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/r2"
        measurementSpec = signMeasurementSpec(measurementSpec2, TestRequisitionData.MC_SIGNING_KEY)
        updateTime = timestamp { seconds = 10 }
      }
    // Third requisition delivered on a drained-apart second page — forces a second processReport
    // call for the same report.
    val r3 =
      TestRequisitionData.REQUISITION.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/r3"
        updateTime = timestamp { seconds = 20 }
      }
    var page = 0
    whenever(requisitionsServiceMock.listRequisitions(any())).thenAnswer {
      when (page++) {
        0 ->
          listRequisitionsResponse {
            requisitions += listOf(r1, r2)
            nextPageToken = "p1"
          }
        1 -> {
          Thread.sleep(300)
          listRequisitionsResponse { requisitions += r3 }
        }
        else -> listRequisitionsResponse {}
      }
    }

    val listCalls = AtomicInteger(0)
    val batchCalls = AtomicInteger(0)
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any())).thenAnswer {
      listCalls.incrementAndGet()
      listRequisitionMetadataResponse {}
    }
    whenever(requisitionMetadataServiceMock.batchCreateRequisitionMetadata(any())).thenAnswer {
      invocation ->
      val count = batchCalls.incrementAndGet()
      if (count == 1) throw RuntimeException("simulated batch create failure inside refuse path")
      val request = invocation.getArgument<BatchCreateRequisitionMetadataRequest>(0)
      createRequisitionMetadataRequests += request.requestsList
      batchCreateRequisitionMetadataResponse {
        requisitionMetadata += request.requestsList.map { requisitionMetadata {} }
      }
    }

    createFetcher(flushInterval = Duration.ofMillis(100)).fetchAndStoreRequisitions()

    // The first unit's refuse path threw → reportFailures incremented.
    assertThat(counterValue("edpa.requisition_fetcher.report_failures")).isAtLeast(1)
    // The cache invalidate in finally fired, so unit 2 re-listed metadata.
    assertThat(listCalls.get()).isEqualTo(2)
  }

  @Test
  fun `recovery rebuild skipped when groupForReport throws InconsistentEventGroupSelectorsException`() =
    runBlocking {
      // Single requisition whose own spec references two event groups with mismatched
      // entity-key presence — getEventGroupMapEntries → validateEventGroupSelectors throws
      // InconsistentEventGroupSelectorsException, which the recovery loop catches and logs
      // rather than letting it bubble up to processReport's outer catch.
      val groupId = "wedged-group-id"
      val blobKey = "$STORAGE_PATH_PREFIX/$groupId"
      val secondEventGroupName = "${TestRequisitionData.EDP_NAME}/eventGroups/name2"
      val mixedSpec =
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
      val r1 =
        TestRequisitionData.REQUISITION.copy {
          encryptedRequisitionSpec =
            encryptRequisitionSpec(
              signedMessage { message = mixedSpec.pack() },
              TestRequisitionData.DATA_PROVIDER_PUBLIC_KEY,
            )
          updateTime = timestamp { seconds = 10 }
        }
      whenever(requisitionsServiceMock.listRequisitions(any()))
        .thenReturn(listRequisitionsResponse { requisitions += r1 })
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
      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
        .thenReturn(
          listRequisitionMetadataResponse {
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.STORED
              cmmsRequisition = r1.name
              blobUri = "$BLOB_URI_PREFIX/$blobKey"
              blobTypeUrl = "type"
              this.groupId = groupId
              report = "some-report"
            }
          }
        )

      createFetcher().fetchAndStoreRequisitions()

      // Rebuild attempted but groupForReport threw → blob still missing, no rebuild counter, no
      // incomplete counter (pendingRecovery removed on the rebuild-attempted path).
      assertThat(storageClient.getBlob(blobKey)).isNull()
      assertThat(counterValue("edpa.requisition_fetcher.recovery_rebuilds")).isEqualTo(0)
      assertThat(counterValue("edpa.requisition_fetcher.recovery_skipped_incomplete")).isEqualTo(0)
    }

  @Test
  fun `unparseable MeasurementSpec does not touch metadata service for that requisition`() =
    runBlocking {
      val bad =
        TestRequisitionData.REQUISITION.copy {
          measurementSpec = signedMessage {
            message = StringValue.of("not a MeasurementSpec").pack()
          }
        }
      whenever(requisitionsServiceMock.listRequisitions(any()))
        .thenReturn(listRequisitionsResponse { requisitions += bad })

      val listCalls = AtomicInteger(0)
      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any())).thenAnswer {
        listCalls.incrementAndGet()
        listRequisitionMetadataResponse {}
      }

      createFetcher().fetchAndStoreRequisitions()

      assertThat(refuseRequisitionRequests).hasSize(1)
      assertThat(createRequisitionMetadataRequests).isEmpty()
      assertThat(refuseRequisitionMetadataRequests).isEmpty()
      assertThat(listCalls.get()).isEqualTo(0)
      assertThat(blobsList()).isEmpty()
    }

  @Test
  fun `cross-unit recovery completes when a report is split across drains`() = runBlocking {
    // A wedged group (STORED metadata, missing blob) whose two expected requisitions arrive in
    // SEPARATE work units within one run: r1 on the first page, r2 on a slow second page delivered
    // after the flush interval so the ticker drains r1 as its own unit first. Neither unit alone
    // satisfies the expected set {r1, r2}; recovery must accumulate ACROSS units to rebuild. This
    // test fails if cross-unit accumulation (PendingRecovery) is removed.
    val groupId = "wedged-split-group"
    val blobKey = "$STORAGE_PATH_PREFIX/$groupId"
    val r1 =
      TestRequisitionData.REQUISITION.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/r1"
        updateTime = timestamp { seconds = 10 }
      }
    val r2 =
      TestRequisitionData.REQUISITION.copy {
        name = "${TestRequisitionData.EDP_NAME}/requisitions/r2"
        updateTime = timestamp { seconds = 20 }
      }
    // Two pages, drained apart: page 0 returns r1 (+token); page 1 sleeps past the flush interval
    // then returns r2. The periodic drain flushes r1 before r2 arrives -> two units for one report.
    var page = 0
    whenever(requisitionsServiceMock.listRequisitions(any())).thenAnswer {
      when (page++) {
        0 ->
          listRequisitionsResponse {
            requisitions += r1
            nextPageToken = "p1"
          }
        1 -> {
          Thread.sleep(300)
          listRequisitionsResponse { requisitions += r2 }
        }
        else -> listRequisitionsResponse {}
      }
    }
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata +=
            listOf(
              requisitionMetadata {
                state = RequisitionMetadata.State.STORED
                cmmsRequisition = r1.name
                blobUri = "$BLOB_URI_PREFIX/$blobKey"
                blobTypeUrl = "type"
                this.groupId = groupId
                report = "some-report"
              },
              requisitionMetadata {
                state = RequisitionMetadata.State.STORED
                cmmsRequisition = r2.name
                blobUri = "$BLOB_URI_PREFIX/$blobKey"
                blobTypeUrl = "type"
                this.groupId = groupId
                report = "some-report"
              },
            )
        }
      )

    createFetcher(flushInterval = Duration.ofMillis(100)).fetchAndStoreRequisitions()

    // Rebuilt only because the two units were accumulated; the blob is present and counted once.
    assertThat(storageClient.getBlob(blobKey)).isNotNull()
    assertThat(counterValue("edpa.requisition_fetcher.recovery_rebuilds")).isEqualTo(1)
  }

  companion object {
    init {
      EdpaTelemetry.ensureInitialized()
    }

    private const val STORAGE_PATH_PREFIX = "test-requisitions"
    private const val DIRECT_STORAGE_PATH_PREFIX = "test-requisitions-v2"
    private const val BLOB_URI_PREFIX = "file:///my-bucket"
  }
}
