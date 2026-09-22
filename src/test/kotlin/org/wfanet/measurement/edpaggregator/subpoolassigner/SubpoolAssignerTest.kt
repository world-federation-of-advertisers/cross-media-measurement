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

package org.wfanet.measurement.edpaggregator.subpoolassigner

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.logging.Handler
import java.util.logging.LogRecord
import java.util.logging.Logger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.telemetry.XmmTraceAttributes
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.rawimpressions.LabelerInputMapper
import org.wfanet.measurement.edpaggregator.rawimpressions.ParquetDigestedEvent
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionSource
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.MarkPoolAssignmentJobSucceededResponseKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.markPoolAssignmentJobSucceededResponse
import org.wfanet.measurement.edpaggregator.v1alpha.poolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.vidRankBuilderParams
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.virtualpeople.common.LabelerInput

private const val UPLOAD = "dataProviders/dp/rawImpressionUploads/up1"
private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml1"
private const val POOL_ASSIGNMENT_JOB =
  "dataProviders/dp/rawImpressionUploads/up1/poolAssignmentJobs/paj-0"
private const val PARENT_NAME =
  "dataProviders/dp/rawImpressionUploads/up1/rawImpressionUploadModelLines/rl1"
private const val QUEUE = "queues/vid-rank-builder"
private val TRACE_CONTEXT =
  mapOf("traceparent" to "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")

private val DEK_GEN = encryptedDek { kekUri = "kek-gen" }
private val DEK_MERGED = encryptedDek { kekUri = "kek-merged" }
private val DEK_SHARD0 = encryptedDek { kekUri = "kek-shard0" }

private val TEMPLATE: VidRankBuilderParams = vidRankBuilderParams {
  dataProvider = "dataProviders/dp"
  rawImpressionUpload = UPLOAD
  modelLine = MODEL_LINE
  totalShards = 1
}

@RunWith(JUnit4::class)
class SubpoolAssignerTest {
  private lateinit var openTelemetry: OpenTelemetrySdk
  private lateinit var spanExporter: InMemorySpanExporter
  private val logRecords = mutableListOf<LogRecord>()
  private val traceLogger = Logger.getLogger(SubpoolAssigner::class.java.name)
  private val logHandler =
    object : Handler() {
      override fun publish(record: LogRecord) {
        logRecords += record
      }

      override fun flush() {}

      override fun close() {}
    }

  @Before
  fun initTelemetry() {
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
    logRecords.clear()
    traceLogger.addHandler(logHandler)
  }

  @After
  fun cleanupTelemetry() {
    traceLogger.removeHandler(logHandler)
    openTelemetry.close()
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
  }

  /** Records cross-collaborator call order so tests can assert e.g. delete-after-flip. */
  private val order = mutableListOf<String>()

  private fun jobResponse(state: PoolAssignmentJob.State) = poolAssignmentJob {
    this.state = state
    etag = "etag-1"
  }

  private fun parent(
    state: RawImpressionUploadModelLine.State,
    poolOffsets: List<Long> = emptyList(),
    withMergedDek: Boolean = false,
  ) = rawImpressionUploadModelLine {
    name = PARENT_NAME
    cmmsModelLine = MODEL_LINE
    this.state = state
    this.poolOffsets += poolOffsets
    maxEventDate = date {
      year = 2026
      month = 6
      day = 15
    }
    if (withMergedDek) {
      encryptedMergedDek = DEK_MERGED
    }
  }

  /** A store mock that records write/merge/delete and hands back [DEK_GEN] from generateDek. */
  private fun storeMock(): SubpoolFingerprintsStore = mock {
    on { generateDek(any()) } doReturn DEK_GEN
    onBlocking { writeBlob(any(), any(), any(), any()) } doAnswer
      {
        order.add("write")
        Unit
      }
    onBlocking { mergeSubpool(any(), any(), any(), any()) } doAnswer
      {
        order.add("merge")
        Unit
      }
    onBlocking { delete(any()) } doAnswer
      {
        order.add("delete")
        Unit
      }
  }

  private fun rankerStubMock(): RankerJobServiceCoroutineStub = mock {
    onBlocking { createRankerJob(any(), any()) } doAnswer
      {
        order.add("ranker")
        rankerJob { name = "$UPLOAD/rankerJobs/rj7" }
      }
  }

  private fun workItemsStubMock(): WorkItemsCoroutineStub = mock {
    onBlocking { createWorkItem(any(), any()) } doAnswer
      {
        order.add("workitem")
        WorkItem.getDefaultInstance()
      }
  }

  private fun assigner(
    store: SubpoolFingerprintsStore,
    poolAssignmentJobsStub: PoolAssignmentJobServiceCoroutineStub,
    modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
    rankerStub: RankerJobServiceCoroutineStub = rankerStubMock(),
    workItemsStub: WorkItemsCoroutineStub = workItemsStubMock(),
    source: RawImpressionSource<ParquetDigestedEvent> = mock(),
    accumulator: SubpoolFingerprintsAccumulator = SubpoolFingerprintsAccumulator(),
    totalShards: Int = 1,
    rpcThrottlers: VidLabelingRpcThrottlers = VidLabelingRpcThrottlersTestHelper.alwaysReady(),
    traceContextProvider: () -> Map<String, String> = { emptyMap() },
  ) =
    SubpoolAssigner(
      rawImpressionSource = source,
      mapper = mock<LabelerInputMapper>(),
      labeler = FakePoolEmitLabeler,
      activeWindow = ActiveWindow(0, Long.MAX_VALUE),
      store = store,
      kekUri = "kek",
      blobPrefix = "maps",
      poolAssignmentJobsStub = poolAssignmentJobsStub,
      rawImpressionUploadModelLinesStub = modelLinesStub,
      rankerJobsStub = rankerStub,
      rawImpressionUploadsStub = mock<RawImpressionUploadServiceCoroutineStub>(),
      workItemsStub = workItemsStub,
      rawImpressionUpload = UPLOAD,
      modelLine = MODEL_LINE,
      poolAssignmentJob = POOL_ASSIGNMENT_JOB,
      shardIndex = 0,
      totalShards = totalShards,
      vidRankBuilderQueue = QUEUE,
      vidRankBuilderParamsTemplate = TEMPLATE,
      rpcThrottlers = rpcThrottlers,
      accumulator = accumulator,
      traceContextProvider = traceContextProvider,
    )

  private fun accumulatorWith(subpoolId: Long): SubpoolFingerprintsAccumulator =
    SubpoolFingerprintsAccumulator().apply { add(subpoolId, 1L, 1) }

  @Test
  fun `non-last shard writes its blobs and marks succeeded without fan-out`() = runBlocking {
    val store = storeMock()
    val ranker = rankerStubMock()
    val workItems = workItemsStubMock()
    val paj =
      mock<PoolAssignmentJobServiceCoroutineStub> {
        onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
          jobResponse(PoolAssignmentJob.State.CREATED)
        // No last_shard_result -> not the last shard out.
        onBlocking { markPoolAssignmentJobSucceeded(any(), any()) } doReturn
          markPoolAssignmentJobSucceededResponse {}
      }
    val ruml = mock<RawImpressionUploadModelLineServiceCoroutineStub>()

    val result =
      assigner(store, paj, ruml, ranker, workItems, accumulator = accumulatorWith(7L)).assign()

    assertThat(result.lastShardOut).isFalse()
    verifyBlocking(store) { writeBlob(any(), any(), any(), any()) }
    verifyBlocking(paj) { markPoolAssignmentJobSucceeded(any(), any()) }
    verifyBlocking(store, never()) { mergeSubpool(any(), any(), any(), any()) }
    verifyBlocking(ranker, never()) { createRankerJob(any(), any()) }
    verifyBlocking(workItems, never()) { createWorkItem(any(), any()) }
    verifyBlocking(ruml, never()) { markRawImpressionUploadModelLineRanking(any(), any()) }
  }

  @Test
  fun `last shard out merges with own DEK, fans out, then deletes after the flip`() = runBlocking {
    val store = storeMock()
    val ranker = rankerStubMock()
    val workItems = workItemsStubMock()
    val paj =
      mock<PoolAssignmentJobServiceCoroutineStub> {
        onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
          jobResponse(PoolAssignmentJob.State.CREATED)
        onBlocking { markPoolAssignmentJobSucceeded(any(), any()) } doReturn
          markPoolAssignmentJobSucceededResponse {
            lastShardResult =
              MarkPoolAssignmentJobSucceededResponseKt.lastShardResult { poolOffsets += 7L }
          }
        onBlocking { listPoolAssignmentJobs(any(), any()) } doReturn
          listPoolAssignmentJobsResponse {
            poolAssignmentJobs += poolAssignmentJob {
              shardIndex = 0
              encryptedDek = DEK_SHARD0
            }
            nextPageToken = ""
          }
      }
    val ruml =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines +=
              parent(RawImpressionUploadModelLine.State.POOL_ASSIGNING, listOf(7L))
            nextPageToken = ""
          }
        onBlocking { markRawImpressionUploadModelLineRanking(any(), any()) } doAnswer
          {
            order.add("flip")
            parent(RawImpressionUploadModelLine.State.RANKING, listOf(7L))
          }
      }
    val recordingThrottlers = VidLabelingRpcThrottlersTestHelper.recording()

    val result =
      assigner(
          store,
          paj,
          ruml,
          ranker,
          workItems,
          accumulator = accumulatorWith(7L),
          rpcThrottlers = recordingThrottlers.throttlers,
        )
        .assign()

    assertThat(result.lastShardOut).isTrue()
    assertThat(order)
      .containsExactly("write", "merge", "ranker", "workitem", "flip", "delete")
      .inOrder()
    // Normal path merges with this VM's freshly generated DEK.
    val dekCaptor = argumentCaptor<EncryptedDek>()
    verifyBlocking(store) { mergeSubpool(any(), any(), dekCaptor.capture(), any()) }
    assertThat(dekCaptor.firstValue).isEqualTo(DEK_GEN)
    assertThat(recordingThrottlers.kingdom.invocationCount).isEqualTo(0)
    assertThat(recordingThrottlers.metadataRead.invocationCount).isEqualTo(3)
    assertThat(recordingThrottlers.metadataWrite.invocationCount).isEqualTo(3)
    assertThat(recordingThrottlers.controlPlane.invocationCount).isEqualTo(1)
  }

  @Test
  fun `missing shard DEK records merge failure identity`() =
    runBlocking<Unit> {
      val store = storeMock()
      val paj =
        mock<PoolAssignmentJobServiceCoroutineStub> {
          onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
            jobResponse(PoolAssignmentJob.State.CREATED)
          onBlocking { markPoolAssignmentJobSucceeded(any(), any()) } doReturn
            markPoolAssignmentJobSucceededResponse {
              lastShardResult =
                MarkPoolAssignmentJobSucceededResponseKt.lastShardResult { poolOffsets += 7L }
            }
          onBlocking { listPoolAssignmentJobs(any(), any()) } doReturn
            listPoolAssignmentJobsResponse {
              poolAssignmentJobs += poolAssignmentJob {
                shardIndex = 0
                encryptedDek = DEK_SHARD0
              }
            }
        }
      val ruml =
        mock<RawImpressionUploadModelLineServiceCoroutineStub> {
          onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
            listRawImpressionUploadModelLinesResponse {
              rawImpressionUploadModelLines +=
                parent(RawImpressionUploadModelLine.State.POOL_ASSIGNING, listOf(7L))
            }
        }

      assertFailsWith<IllegalArgumentException> {
        assigner(store, paj, ruml, accumulator = accumulatorWith(7L), totalShards = 2).assign()
      }

      val failureLog = logRecords.single { it.message.contains("pool_assignment.merge_failed") }
      assertThat(failureLog.message).contains("xmm.edpa.pool_offset=7")
      assertThat(failureLog.message).contains("xmm.edpa.shard_index=1")
      assertThat(failureLog.message).contains("xmm.lifecycle.stage=pool_assignment_merge")
      assertThat(failureLog.message).contains("xmm.outcome=failed")
      assertThat(failureLog.message).contains("xmm.error.type=IllegalArgumentException")
      assertThat(failureLog.message).doesNotContain("xmm.error.code=")
    }

  @Test
  fun `early successful shard retries until the parent reaches POOL_ASSIGNING`() = runBlocking {
    val store = storeMock()
    val ranker = rankerStubMock()
    val workItems = workItemsStubMock()
    val parentState = AtomicReference(RawImpressionUploadModelLine.State.CREATED)
    val paj =
      mock<PoolAssignmentJobServiceCoroutineStub> {
        onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
          jobResponse(PoolAssignmentJob.State.SUCCEEDED)
        onBlocking { listPoolAssignmentJobs(any(), any()) } doReturn
          listPoolAssignmentJobsResponse {
            poolAssignmentJobs += poolAssignmentJob {
              shardIndex = 0
              encryptedDek = DEK_SHARD0
            }
          }
      }
    val ruml =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doAnswer
          {
            listRawImpressionUploadModelLinesResponse {
              rawImpressionUploadModelLines +=
                parent(parentState.get(), listOf(7L), withMergedDek = true)
            }
          }
        onBlocking { markRawImpressionUploadModelLineRanking(any(), any()) } doReturn
          parent(RawImpressionUploadModelLine.State.RANKING, listOf(7L), withMergedDek = true)
      }
    val subject = assigner(store, paj, ruml, ranker, workItems)

    val exception = assertFailsWith<IllegalStateException> { subject.assign() }

    assertThat(exception).hasMessageThat().contains("has not reached POOL_ASSIGNING")
    verifyBlocking(store, never()) { mergeSubpool(any(), any(), any(), any()) }
    verifyBlocking(ranker, never()) { createRankerJob(any(), any()) }

    parentState.set(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
    val result = subject.assign()

    assertThat(result.lastShardOut).isTrue()
    verifyBlocking(store) { mergeSubpool(any(), any(), any(), any()) }
    verifyBlocking(ranker) { createRankerJob(any(), any()) }
    verifyBlocking(ruml) { markRawImpressionUploadModelLineRanking(any(), any()) }
  }

  @Test
  fun `recovery on already-succeeded shard that was not last-out does nothing`() = runBlocking {
    val store = storeMock()
    val ranker = rankerStubMock()
    val workItems = workItemsStubMock()
    val paj =
      mock<PoolAssignmentJobServiceCoroutineStub> {
        onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
          jobResponse(PoolAssignmentJob.State.SUCCEEDED)
      }
    val ruml =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
          listRawImpressionUploadModelLinesResponse {
            // Empty pool_offsets -> this shard was not the last out.
            rawImpressionUploadModelLines +=
              parent(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
            nextPageToken = ""
          }
      }

    val result = assigner(store, paj, ruml, ranker, workItems).assign()

    assertThat(result.lastShardOut).isFalse()
    assertThat(result.outcome).isEqualTo("already_completed")
    assertThat(
        spanExporter.finishedSpanItems
          .single { it.name == "edpa.vid_labeling.pool_assignment" }
          .attributes
          .get(XmmTraceAttributes.OUTCOME)
      )
      .isEqualTo("already_completed")
    verifyBlocking(store, never()) { mergeSubpool(any(), any(), any(), any()) }
    verifyBlocking(ranker, never()) { createRankerJob(any(), any()) }
  }

  @Test
  fun `completed shard output remains in trace when a sibling write fails`() =
    runBlocking<Unit> {
      val writes = AtomicInteger()
      val store =
        mock<SubpoolFingerprintsStore> {
          on { generateDek(any()) } doReturn DEK_GEN
          onBlocking { writeBlob(any(), any(), any(), any()) } doAnswer
            {
              if (writes.incrementAndGet() == 1) Unit else error("write failed")
            }
        }
      val paj =
        mock<PoolAssignmentJobServiceCoroutineStub> {
          onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
            jobResponse(PoolAssignmentJob.State.CREATED)
        }
      val accumulator =
        SubpoolFingerprintsAccumulator().apply {
          add(7L, 1L, 1)
          add(11L, 2L, 1)
        }

      assertFailsWith<IllegalStateException> {
        assigner(store, paj, mock(), accumulator = accumulator).assign()
      }

      val phaseSpan =
        spanExporter.finishedSpanItems.single { it.name == "edpa.vid_labeling.pool_assignment" }
      val outputs =
        phaseSpan.events.filter { it.name == "edpa.vid_labeling.pool_assignment.shard_output" }
      assertThat(outputs).isNotEmpty()
      assertThat(outputs.map { it.attributes.get(XmmTraceAttributes.OUTCOME) }.toSet())
        .containsExactly("written")
      val failureLog =
        logRecords.single { it.message.contains("pool_assignment.shard_output_failed") }
      assertThat(failureLog.message).contains("xmm.lifecycle.stage=pool_assignment_output")
      assertThat(failureLog.message).contains("xmm.edpa.pool_offset=")
      assertThat(failureLog.message).contains("xmm.edpa.shard_index=0")
      assertThat(failureLog.message).contains("xmm.error.type=IllegalStateException")
    }

  @Test
  fun `recovery short-circuits when the parent already advanced past POOL_ASSIGNING`() =
    runBlocking {
      val store = storeMock()
      val ranker = rankerStubMock()
      val workItems = workItemsStubMock()
      val paj =
        mock<PoolAssignmentJobServiceCoroutineStub> {
          onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
            jobResponse(PoolAssignmentJob.State.SUCCEEDED)
        }
      val ruml =
        mock<RawImpressionUploadModelLineServiceCoroutineStub> {
          onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
            listRawImpressionUploadModelLinesResponse {
              rawImpressionUploadModelLines +=
                parent(RawImpressionUploadModelLine.State.RANKING, listOf(7L), withMergedDek = true)
              nextPageToken = ""
            }
        }

      val result = assigner(store, paj, ruml, ranker, workItems).assign()

      assertThat(result.lastShardOut).isTrue()
      assertThat(result.outcome).isEqualTo("already_completed")
      verifyBlocking(store, never()) { mergeSubpool(any(), any(), any(), any()) }
      verifyBlocking(ranker, never()) { createRankerJob(any(), any()) }
      verifyBlocking(workItems, never()) { createWorkItem(any(), any()) }
    }

  @Test
  fun `late successful shard does not fan out after the parent was marked FAILED`() = runBlocking {
    val store = storeMock()
    val ranker = rankerStubMock()
    val workItems = workItemsStubMock()
    val paj =
      mock<PoolAssignmentJobServiceCoroutineStub> {
        onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
          jobResponse(PoolAssignmentJob.State.CREATED)
        onBlocking { markPoolAssignmentJobSucceeded(any(), any()) } doReturn
          markPoolAssignmentJobSucceededResponse {
            lastShardResult =
              MarkPoolAssignmentJobSucceededResponseKt.lastShardResult { poolOffsets += 7L }
          }
      }
    val ruml =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines +=
              parent(RawImpressionUploadModelLine.State.FAILED, listOf(7L), withMergedDek = true)
          }
      }

    val result = assigner(store, paj, ruml, ranker, workItems).assign()

    assertThat(result.lastShardOut).isTrue()
    verifyBlocking(store, never()) { mergeSubpool(any(), any(), any(), any()) }
    verifyBlocking(ranker, never()) { createRankerJob(any(), any()) }
    verifyBlocking(workItems, never()) { createWorkItem(any(), any()) }
    verifyBlocking(ruml, never()) { markRawImpressionUploadModelLineRanking(any(), any()) }
  }

  @Test
  fun `recovery re-runs the last-shard-out reusing the persisted merged DEK`() = runBlocking {
    val store = storeMock()
    val ranker = rankerStubMock()
    val workItems = workItemsStubMock()
    val paj =
      mock<PoolAssignmentJobServiceCoroutineStub> {
        onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
          jobResponse(PoolAssignmentJob.State.SUCCEEDED)
        onBlocking { listPoolAssignmentJobs(any(), any()) } doReturn
          listPoolAssignmentJobsResponse {
            poolAssignmentJobs += poolAssignmentJob {
              shardIndex = 0
              encryptedDek = DEK_SHARD0
            }
            nextPageToken = ""
          }
      }
    val ruml =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines +=
              parent(
                RawImpressionUploadModelLine.State.POOL_ASSIGNING,
                listOf(7L),
                withMergedDek = true,
              )
            nextPageToken = ""
          }
        onBlocking { markRawImpressionUploadModelLineRanking(any(), any()) } doAnswer
          {
            order.add("flip")
            parent(RawImpressionUploadModelLine.State.RANKING, listOf(7L), withMergedDek = true)
          }
      }

    val result = assigner(store, paj, ruml, ranker, workItems).assign()

    assertThat(result.lastShardOut).isTrue()
    // No re-read/re-write of per-shard blobs; delete happens AFTER the flip (#3999 regression
    // guard).
    assertThat(order).containsExactly("merge", "ranker", "workitem", "flip", "delete").inOrder()
    // Re-merge reuses the persisted DEK so it stays consistent with the already-written blobs.
    val dekCaptor = argumentCaptor<EncryptedDek>()
    verifyBlocking(store) { mergeSubpool(any(), any(), dekCaptor.capture(), any()) }
    assertThat(dekCaptor.firstValue).isEqualTo(DEK_MERGED)
  }

  @Test
  fun `recovery fails fast when pool_offsets are set but the merged DEK is missing`() =
    runBlocking {
      val store = storeMock()
      val paj =
        mock<PoolAssignmentJobServiceCoroutineStub> {
          onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
            jobResponse(PoolAssignmentJob.State.SUCCEEDED)
        }
      val ruml =
        mock<RawImpressionUploadModelLineServiceCoroutineStub> {
          onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
            listRawImpressionUploadModelLinesResponse {
              // pool_offsets set but no encrypted_merged_dek.
              rawImpressionUploadModelLines +=
                parent(RawImpressionUploadModelLine.State.POOL_ASSIGNING, listOf(7L))
              nextPageToken = ""
            }
        }

      val thrown =
        try {
          assigner(store, paj, ruml).assign()
          null
        } catch (e: Exception) {
          e
        }

      assertThat(thrown).isInstanceOf(IllegalArgumentException::class.java)
      verifyBlocking(store, never()) { mergeSubpool(any(), any(), any(), any()) }
    }

  @Test
  fun `transient flip failure propagates and the temp blobs are not deleted`() = runBlocking {
    val store = storeMock()
    val paj =
      mock<PoolAssignmentJobServiceCoroutineStub> {
        onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
          jobResponse(PoolAssignmentJob.State.SUCCEEDED)
        onBlocking { listPoolAssignmentJobs(any(), any()) } doReturn
          listPoolAssignmentJobsResponse {
            poolAssignmentJobs += poolAssignmentJob {
              shardIndex = 0
              encryptedDek = DEK_SHARD0
            }
            nextPageToken = ""
          }
      }
    val ruml =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines +=
              parent(
                RawImpressionUploadModelLine.State.POOL_ASSIGNING,
                listOf(7L),
                withMergedDek = true,
              )
            nextPageToken = ""
          }
        onBlocking { markRawImpressionUploadModelLineRanking(any(), any()) } doAnswer
          {
            throw StatusException(Status.UNAVAILABLE)
          }
      }

    val thrown =
      try {
        assigner(store, paj, ruml).assign()
        null
      } catch (e: Exception) {
        e
      }

    assertThat(thrown).isInstanceOf(StatusException::class.java)
    // The flip is the completion marker; on a transient failure we must NOT delete the merge
    // inputs.
    verifyBlocking(store, never()) { delete(any()) }
    // The worker never marks the job FAILED itself — the DLQ listener owns the terminal FAILED
    // transition on retry exhaustion; the failure simply propagates so the framework nacks.
    verifyBlocking(paj, never()) { markPoolAssignmentJobFailed(any(), any()) }
  }

  @Test
  fun `already-advanced flip precondition is swallowed and cleanup still runs`() = runBlocking {
    val store = storeMock()
    val parentState = AtomicReference(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
    val paj =
      mock<PoolAssignmentJobServiceCoroutineStub> {
        onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
          jobResponse(PoolAssignmentJob.State.SUCCEEDED)
        onBlocking { listPoolAssignmentJobs(any(), any()) } doReturn
          listPoolAssignmentJobsResponse {
            poolAssignmentJobs += poolAssignmentJob {
              shardIndex = 0
              encryptedDek = DEK_SHARD0
            }
            nextPageToken = ""
          }
      }
    val ruml =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doAnswer
          {
            listRawImpressionUploadModelLinesResponse {
              rawImpressionUploadModelLines +=
                parent(parentState.get(), listOf(7L), withMergedDek = true)
              nextPageToken = ""
            }
          }
        onBlocking { markRawImpressionUploadModelLineRanking(any(), any()) } doAnswer
          {
            parentState.set(RawImpressionUploadModelLine.State.RANKING)
            throw StatusException(Status.FAILED_PRECONDITION)
          }
      }

    val result = assigner(store, paj, ruml).assign()

    assertThat(result.lastShardOut).isTrue()
    verifyBlocking(store) { delete(any()) }
  }

  @Test
  fun `ranking transition conflict propagates while the parent remains POOL_ASSIGNING`() =
    runBlocking {
      val store = storeMock()
      val paj =
        mock<PoolAssignmentJobServiceCoroutineStub> {
          onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
            jobResponse(PoolAssignmentJob.State.SUCCEEDED)
          onBlocking { listPoolAssignmentJobs(any(), any()) } doReturn
            listPoolAssignmentJobsResponse {
              poolAssignmentJobs += poolAssignmentJob {
                shardIndex = 0
                encryptedDek = DEK_SHARD0
              }
            }
        }
      val ruml =
        mock<RawImpressionUploadModelLineServiceCoroutineStub> {
          onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
            listRawImpressionUploadModelLinesResponse {
              rawImpressionUploadModelLines +=
                parent(
                  RawImpressionUploadModelLine.State.POOL_ASSIGNING,
                  listOf(7L),
                  withMergedDek = true,
                )
            }
          onBlocking { markRawImpressionUploadModelLineRanking(any(), any()) } doAnswer
            {
              throw StatusException(Status.ABORTED)
            }
        }

      assertFailsWith<StatusException> { assigner(store, paj, ruml).assign() }

      verifyBlocking(store, never()) { delete(any()) }
    }

  @Test
  fun `last shard out stamps each subpool's ranked size from the labeler`() =
    runBlocking<Unit> {
      val store = storeMock()
      val ranker = rankerStubMock()
      val workItems = workItemsStubMock()
      val paj =
        mock<PoolAssignmentJobServiceCoroutineStub> {
          onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
            jobResponse(PoolAssignmentJob.State.CREATED)
          onBlocking { markPoolAssignmentJobSucceeded(any(), any()) } doReturn
            markPoolAssignmentJobSucceededResponse {
              lastShardResult =
                MarkPoolAssignmentJobSucceededResponseKt.lastShardResult {
                  poolOffsets += listOf(7L, 11L)
                }
            }
          onBlocking { listPoolAssignmentJobs(any(), any()) } doReturn
            listPoolAssignmentJobsResponse {
              poolAssignmentJobs += poolAssignmentJob {
                shardIndex = 0
                encryptedDek = DEK_SHARD0
              }
              nextPageToken = ""
            }
        }
      val ruml =
        mock<RawImpressionUploadModelLineServiceCoroutineStub> {
          onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
            listRawImpressionUploadModelLinesResponse {
              rawImpressionUploadModelLines +=
                parent(RawImpressionUploadModelLine.State.POOL_ASSIGNING, listOf(7L, 11L))
              nextPageToken = ""
            }
          onBlocking { markRawImpressionUploadModelLineRanking(any(), any()) } doReturn
            parent(RawImpressionUploadModelLine.State.RANKING, listOf(7L, 11L))
        }
      val accumulator =
        SubpoolFingerprintsAccumulator().apply {
          add(7L, 1L, 1)
          add(11L, 2L, 1)
        }

      assigner(
          store,
          paj,
          ruml,
          ranker,
          workItems,
          accumulator = accumulator,
          traceContextProvider = { TRACE_CONTEXT },
        )
        .assign()

      // One WorkItem per subpool; the union of their stamped ranked sizes must match the labeler
      // one-to-one (FakePoolEmitLabeler returns 1000 + offset), so a key-swap or dropped offset
      // fails.
      val captor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItems, times(2)) { createWorkItem(captor.capture(), any()) }
      val stamped: Map<Long, Int> =
        captor.allValues
          .map {
            it.workItem.workItemParams
              .unpack(WorkItemParams::class.java)
              .appParams
              .unpack(VidRankBuilderParams::class.java)
          }
          .flatMap { it.subpoolRankedSizesMap.entries }
          .associate { it.key to it.value }
      assertThat(stamped).containsExactly(7L, 1007, 11L, 1011)
      for (request in captor.allValues) {
        assertThat(
            request.workItem.workItemParams.unpack(WorkItemParams::class.java).traceContextMap
          )
          .containsExactlyEntriesIn(TRACE_CONTEXT)
      }
      val phaseSpan =
        spanExporter.finishedSpanItems.single { it.name == "edpa.vid_labeling.pool_assignment" }
      assertThat(phaseSpan.attributes.get(VidLabelingTraceAttributes.PIPELINE_PHASE))
        .isEqualTo("phase0")
      assertThat(phaseSpan.attributes.get(XmmTraceAttributes.OUTCOME)).isEqualTo("succeeded")
      assertThat(
          phaseSpan.attributes.get(AttributeKey.longKey("xmm.edpa.pool_assignment.subpool_count"))
        )
        .isEqualTo(2L)
      assertThat(
          phaseSpan.attributes.get(
            AttributeKey.booleanKey("xmm.edpa.pool_assignment.last_shard_out")
          )
        )
        .isTrue()
      for (name in listOf("labeled_count", "dropped_count", "unrouted_count")) {
        assertThat(phaseSpan.attributes.get(AttributeKey.longKey("xmm.edpa.pool_assignment.$name")))
          .isEqualTo(0L)
      }
      val finalizeSpan =
        spanExporter.finishedSpanItems.single {
          it.name == "edpa.vid_labeling.pool_assignment.finalize"
        }
      assertThat(finalizeSpan.events.map { it.name })
        .containsAtLeast(
          "edpa.vid_labeling.pool_assignment.merged_subpool",
          "edpa.vid_labeling.pool_assignment.ranker_job",
          "edpa.vid_labeling.pool_assignment.work_item",
          "edpa.vid_labeling.pool_assignment.parent_transition",
        )
      val rankerEvents =
        finalizeSpan.events.filter { it.name == "edpa.vid_labeling.pool_assignment.ranker_job" }
      assertThat(
          rankerEvents.mapNotNull { it.attributes.get(VidLabelingTraceAttributes.RANKER_JOB_NAME) }
        )
        .hasSize(2)
      assertThat(rankerEvents.map { it.attributes.get(XmmTraceAttributes.OUTCOME) }.toSet())
        .containsExactly("resolved")
      val workItemEvents =
        finalizeSpan.events.filter { it.name == "edpa.vid_labeling.pool_assignment.work_item" }
      assertThat(workItemEvents.mapNotNull { it.attributes.get(XmmTraceAttributes.WORK_ITEM_NAME) })
        .hasSize(2)
      assertThat(workItemEvents.map { it.attributes.get(XmmTraceAttributes.OUTCOME) }.toSet())
        .containsExactly("created")
      assertThat(logRecords.map { it.message })
        .containsAtLeastElementsIn(
          listOf(
            "event=edpa.vid_labeling.pool_assignment_completed " +
              "xmm.edpa.raw_impression_upload.name=$UPLOAD " +
              "xmm.model_line.name=$MODEL_LINE " +
              "xmm.edpa.pool_assignment_job.name=$POOL_ASSIGNMENT_JOB " +
              "xmm.edpa.pipeline.phase=phase0 " +
              "xmm.lifecycle.stage=pool_assignment xmm.outcome=succeeded"
          )
        )
      assertThat(logRecords.any { it.message.contains("pool_assignment.merged_subpool") }).isTrue()
      assertThat(logRecords.any { it.message.contains("pool_assignment.ranker_job") }).isTrue()
      assertThat(logRecords.any { it.message.contains("pool_assignment.work_item") }).isTrue()
      assertThat(logRecords.any { it.message.contains("pool_assignment.parent_transition") })
        .isTrue()
      val mergedEvents =
        finalizeSpan.events.filter { it.name == "edpa.vid_labeling.pool_assignment.merged_subpool" }
      assertThat(
          mergedEvents.mapNotNull {
            it.attributes.get(AttributeKey.longKey("xmm.edpa.pool_offset"))
          }
        )
        .containsExactly(7L, 11L)
      assertThat(mergedEvents.map { it.attributes.get(XmmTraceAttributes.OUTCOME) }.toSet())
        .containsExactly("written")
      val transition =
        finalizeSpan.events.single {
          it.name == "edpa.vid_labeling.pool_assignment.parent_transition"
        }
      assertThat(transition.attributes.get(XmmTraceAttributes.OUTCOME)).isEqualTo("ranking")
    }

  @Test
  fun `last shard out merges multiple shards and subpools into the correct union`() =
    runBlocking<Unit> {
      // A REAL store (real streaming AEAD storage + KMS) so the merge produces readable blobs.
      AeadConfig.register()
      StreamingAeadConfig.register()
      val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
      val kmsClient =
        FakeKmsClient().apply {
          setAead(
            kekUri,
            KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM")).getPrimitive(Aead::class.java),
          )
        }
      val storageClient = InMemoryStorageClient()
      val store = SubpoolFingerprintsStore(storageClient, kmsClient)

      // Two shards, each with its own DEK; two subpools (7, 8) with disjoint fingerprints.
      val dek0 = store.generateDek(kekUri)
      val dek1 = store.generateDek(kekUri)
      val mergedDek = store.generateDek(kekUri)
      suspend fun writeShard(shard: Int, dek: EncryptedDek, offset: Long, fps: List<ByteArray>) =
        store.writeBlob(
          SubpoolFingerprintsStore.shardSubpoolKey("maps", UPLOAD, MODEL_LINE, shard, offset),
          dek,
          offset,
          flowOf(pack(fps)),
        )
      writeShard(0, dek0, 7L, listOf(fp(0x11), fp(0x22)))
      writeShard(1, dek1, 7L, listOf(fp(0x33)))
      writeShard(0, dek0, 8L, listOf(fp(0x44)))
      writeShard(1, dek1, 8L, listOf(fp(0x55), fp(0x66)))

      // Drive the last-shard-out via the recovery path (already-SUCCEEDED shard, parent still
      // POOL_ASSIGNING with the persisted merged DEK and pool offsets).
      val paj =
        mock<PoolAssignmentJobServiceCoroutineStub> {
          onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
            jobResponse(PoolAssignmentJob.State.SUCCEEDED)
          onBlocking { listPoolAssignmentJobs(any(), any()) } doReturn
            listPoolAssignmentJobsResponse {
              poolAssignmentJobs += poolAssignmentJob {
                shardIndex = 0
                encryptedDek = dek0
              }
              poolAssignmentJobs += poolAssignmentJob {
                shardIndex = 1
                encryptedDek = dek1
              }
              nextPageToken = ""
            }
        }
      val ruml =
        mock<RawImpressionUploadModelLineServiceCoroutineStub> {
          onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
            listRawImpressionUploadModelLinesResponse {
              rawImpressionUploadModelLines += rawImpressionUploadModelLine {
                name = PARENT_NAME
                cmmsModelLine = MODEL_LINE
                state = RawImpressionUploadModelLine.State.POOL_ASSIGNING
                poolOffsets += listOf(7L, 8L)
                maxEventDate = date {
                  year = 2026
                  month = 6
                  day = 15
                }
                encryptedMergedDek = mergedDek
              }
              nextPageToken = ""
            }
          onBlocking { markRawImpressionUploadModelLineRanking(any(), any()) } doReturn
            parent(RawImpressionUploadModelLine.State.RANKING, listOf(7L, 8L), withMergedDek = true)
        }

      assigner(store, paj, ruml, totalShards = 2).assign()

      // Each merged subpool blob is the disjoint union of its shards, order-independent.
      assertThat(fingerprintSet(store, mergedKey(7L), mergedDek))
        .containsExactly(bs(fp(0x11)), bs(fp(0x22)), bs(fp(0x33)))
      assertThat(fingerprintSet(store, mergedKey(8L), mergedDek))
        .containsExactly(bs(fp(0x44)), bs(fp(0x55)), bs(fp(0x66)))
    }

  private fun pack(fingerprints: List<ByteArray>): ByteString {
    val out = ByteString.newOutput()
    fingerprints.forEach {
      require(it.size == 12)
      out.write(it)
    }
    return out.toByteString()
  }

  private fun fp(fill: Int): ByteArray = ByteArray(12) { fill.toByte() }

  private fun bs(bytes: ByteArray): ByteString = ByteString.copyFrom(bytes)

  private fun mergedKey(offset: Long): String =
    SubpoolFingerprintsStore.mergedSubpoolKey("maps", UPLOAD, MODEL_LINE, offset)

  /** Decrypts [key] and returns its 12-byte fingerprints as a SET (order-independent). */
  private suspend fun fingerprintSet(
    store: SubpoolFingerprintsStore,
    key: String,
    dek: EncryptedDek,
  ): Set<ByteString> {
    val set = mutableSetOf<ByteString>()
    store.readBlob(key, dek).toList().forEach { record ->
      val bytes = record.fingerprints
      var i = 0
      while (i < bytes.size()) {
        set.add(bytes.substring(i, i + 12))
        i += 12
      }
    }
    return set
  }

  private object FakePoolEmitLabeler : PoolEmitLabeler {
    override fun emit(input: LabelerInput, onPoolOffset: (Long) -> Unit): Int = 0

    // Distinct per offset so a key-swap / dropped-offset regression in the rankedSize stamping is
    // observable (a constant would hide it).
    override fun rankedSize(poolOffset: Long): Int = (1000 + poolOffset).toInt()
  }
}
