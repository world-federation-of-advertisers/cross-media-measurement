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

package org.wfanet.measurement.edpaggregator.vidlabeling

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Timestamp
import com.google.protobuf.kotlin.unpack
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import java.time.Instant
import java.util.logging.Handler
import java.util.logging.LogRecord
import java.util.logging.Logger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelShardKt.modelBlob
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt
import org.wfanet.measurement.api.v2alpha.listModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.listModelShardsResponse
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelShard
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.telemetry.XmmTraceAttributes
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreatePoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadModelLinePoolAssigningRequest
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ScalarColumn
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreatePoolAssignmentJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.poolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.subpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.transportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

@RunWith(JUnit4::class)
class VidLabelingDispatchSequencerTest {
  private lateinit var openTelemetry: OpenTelemetrySdk
  private lateinit var spanExporter: InMemorySpanExporter
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
    rootLogger.addHandler(logHandler)
  }

  @After
  fun cleanupTelemetry() {
    rootLogger.removeHandler(logHandler)
    openTelemetry.close()
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
  }

  private val rawImpressionUploadService:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase =
    mockService()
  private val rawImpressionUploadModelLineService:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase =
    mockService()
  private val workItemsService: WorkItemsGrpcKt.WorkItemsCoroutineImplBase = mockService()
  private val poolAssignmentJobService:
    PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineImplBase =
    mockService()
  private val rawImpressionUploadFileService:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase =
    mockService()
  private val vidLabelingJobService:
    VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase =
    mockService()
  private val rankIndexBlobService:
    RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineImplBase =
    mockService()
  private val uploadHealingOperationService:
    UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase =
    mockService()
  private val modelRolloutsService: ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase =
    mockService()
  private val modelShardsService: ModelShardsGrpcKt.ModelShardsCoroutineImplBase = mockService()
  private val modelLinesService: ModelLinesGrpcKt.ModelLinesCoroutineImplBase = mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(rawImpressionUploadService)
    addService(rawImpressionUploadModelLineService)
    addService(workItemsService)
    addService(poolAssignmentJobService)
    addService(modelRolloutsService)
    addService(modelShardsService)
    addService(modelLinesService)
    addService(rawImpressionUploadFileService)
    addService(vidLabelingJobService)
    addService(rankIndexBlobService)
    addService(uploadHealingOperationService)
  }

  private val rawImpressionUploadStub by lazy {
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }
  private val rawImpressionUploadModelLineStub by lazy {
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }
  private val workItemsStub by lazy {
    WorkItemsGrpcKt.WorkItemsCoroutineStub(grpcTestServerRule.channel)
  }
  private val poolAssignmentJobStub by lazy {
    PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub(grpcTestServerRule.channel)
  }
  private val modelRolloutsStub by lazy {
    ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub(grpcTestServerRule.channel)
  }
  private val modelShardsStub by lazy {
    ModelShardsGrpcKt.ModelShardsCoroutineStub(grpcTestServerRule.channel)
  }
  private val modelLinesStub by lazy {
    ModelLinesGrpcKt.ModelLinesCoroutineStub(grpcTestServerRule.channel)
  }
  private val rawImpressionUploadFileStub by lazy {
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }
  private val vidLabelingJobStub by lazy {
    VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(grpcTestServerRule.channel)
  }
  private val rankIndexBlobStub by lazy {
    RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub(grpcTestServerRule.channel)
  }
  private val uploadHealingOperationStub by lazy {
    UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  private fun createSequencer(
    numberOfShards: Int = NUMBER_OF_SHARDS,
    subpoolAssignerParamsTemplate: SubpoolAssignerParams = SUBPOOL_ASSIGNER_PARAMS_TEMPLATE,
    rpcThrottlers: VidLabelingRpcThrottlers = VidLabelingRpcThrottlersTestHelper.alwaysReady(),
    traceContextProvider: () -> Map<String, String> = { emptyMap() },
  ): VidLabelingDispatchSequencer =
    VidLabelingDispatchSequencer(
      rawImpressionUploadStub = rawImpressionUploadStub,
      rawImpressionUploadModelLineStub = rawImpressionUploadModelLineStub,
      rankIndexBlobStub = rankIndexBlobStub,
      uploadHealingOperationStub = uploadHealingOperationStub,
      workItemsStub = workItemsStub,
      poolAssignmentJobStub = poolAssignmentJobStub,
      modelRolloutsStub = modelRolloutsStub,
      modelShardsStub = modelShardsStub,
      modelLinesStub = modelLinesStub,
      dataProviderName = DATA_PROVIDER,
      vidLabelerParamsTemplate = VID_LABELER_PARAMS_TEMPLATE,
      subpoolAssignerParamsTemplate = subpoolAssignerParamsTemplate,
      queueName = QUEUE_NAME,
      poolAssignerQueueName = POOL_ASSIGNER_QUEUE_NAME,
      numberOfShards = numberOfShards,
      modelLineConfigs = MODEL_LINE_CONFIGS,
      rawImpressionUploadFileStub = rawImpressionUploadFileStub,
      vidLabelingJobStub = vidLabelingJobStub,
      maxFileBatchSizeBytes = MAX_FILE_BATCH_SIZE_BYTES,
      rpcThrottlers = rpcThrottlers,
      traceContextProvider = traceContextProvider,
    )

  /**
   * Stubs `listRawImpressionUploads` to return [active] for the `ACTIVE` filter and [created] for
   * the `CREATED` filter.
   */
  private suspend fun stubUploads(
    active: List<RawImpressionUpload> = emptyList(),
    created: List<RawImpressionUpload> = emptyList(),
  ) {
    whenever(rawImpressionUploadService.listRawImpressionUploads(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<ListRawImpressionUploadsRequest>(0)
      val uploads =
        when (request.filter.stateInList.firstOrNull()) {
          RawImpressionUpload.State.ACTIVE -> active
          RawImpressionUpload.State.CREATED -> created
          else -> emptyList()
        }
      listRawImpressionUploadsResponse { rawImpressionUploads += uploads }
    }
  }

  private suspend fun stubGetUploads(vararg uploads: RawImpressionUpload) {
    val uploadsByName = uploads.associateBy { it.name }
    whenever(rawImpressionUploadService.getRawImpressionUpload(any())).thenAnswer { invocation ->
      uploadsByName.getValue(invocation.getArgument<GetRawImpressionUploadRequest>(0).name)
    }
  }

  private suspend fun stubHealingOperation(operation: UploadHealingOperation) {
    whenever(uploadHealingOperationService.getUploadHealingOperation(any())).thenReturn(operation)
  }

  private suspend fun stubLiveSnapshots(vararg keys: Pair<String, String>) {
    val live = keys.toSet()
    whenever(rankIndexBlobService.listRankIndexBlobs(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<ListRankIndexBlobsRequest>(0)
      listRankIndexBlobsResponse {
        if (request.parent to request.filter.cmmsModelLine in live) {
          rankIndexBlobs += rankIndexBlob {
            name = "${request.parent}/rankIndexBlobs/snapshot"
            blobType = RankIndexBlob.BlobType.SNAPSHOT
            cmmsModelLine = request.filter.cmmsModelLine
          }
        }
      }
    }
  }

  /** Stubs `listRawImpressionUploadModelLines` to return [modelLines] for any parent upload. */
  private suspend fun stubModelLines(vararg modelLines: RawImpressionUploadModelLine) {
    whenever(rawImpressionUploadModelLineService.listRawImpressionUploadModelLines(any()))
      .thenReturn(
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += modelLines.toList()
        }
      )
  }

  /** Stubs `listRawImpressionUploadModelLines` per parent upload name. */
  private suspend fun stubModelLinesByParent(
    modelLinesByParent: Map<String, List<RawImpressionUploadModelLine>>
  ) {
    whenever(rawImpressionUploadModelLineService.listRawImpressionUploadModelLines(any()))
      .thenAnswer { invocation ->
        val parent = invocation.getArgument<ListRawImpressionUploadModelLinesRequest>(0).parent
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += modelLinesByParent[parent].orEmpty()
        }
      }
  }

  /** Builds a model line under [uploadName] for [cmmsModelLineName] in [state]. */
  private fun modelLine(
    uploadName: String,
    cmmsModelLineName: String,
    state: RawImpressionUploadModelLine.State,
    id: String = "ml",
  ) = rawImpressionUploadModelLine {
    name = "$uploadName/modelLines/$id"
    cmmsModelLine = cmmsModelLineName
    this.state = state
    etag = ETAG
  }

  /** Stubs the ModelRollout -> ModelShard resolution chain. */
  private suspend fun stubShardResolution(memoized: Boolean) {
    whenever(modelRolloutsService.listModelRollouts(any()))
      .thenReturn(
        listModelRolloutsResponse { modelRollouts += modelRollout { modelRelease = MODEL_RELEASE } }
      )
    whenever(modelShardsService.listModelShards(any()))
      .thenReturn(
        listModelShardsResponse {
          modelShards += modelShard {
            name = "$DATA_PROVIDER/modelShards/ms1"
            modelRelease = MODEL_RELEASE
            modelBlob = modelBlob { modelBlobPath = MODEL_BLOB_PATH }
            memoizedVidAssignmentEnabled = memoized
          }
        }
      )
  }

  private suspend fun stubMarkTransitions() {
    whenever(rawImpressionUploadModelLineService.markRawImpressionUploadModelLineLabeling(any()))
      .thenReturn(rawImpressionUploadModelLine {})
  }

  /**
   * Stubs the non-memoized fan-out RPCs: `listRawImpressionUploadFiles` returns [NUMBER_OF_SHARDS]
   * files each sized at the batch threshold (so the bin-packer opens one batch per file), and
   * `batchCreateVidLabelingJobs` echoes one job per request named
   * `${parent}/vidLabelingJobs/job-$i` (preserving the request's `cmms_model_lines` +
   * `raw_impression_upload_files`).
   */
  private suspend fun stubNonMemoizedFilesAndJobs() {
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any())).thenAnswer {
      invocation ->
      val parent = invocation.getArgument<ListRawImpressionUploadFilesRequest>(0).parent
      listRawImpressionUploadFilesResponse {
        for (i in 0 until NUMBER_OF_SHARDS) {
          rawImpressionUploadFiles += rawImpressionUploadFile {
            name = "$parent/rawImpressionUploadFiles/file-$i"
            sizeBytes = MAX_FILE_BATCH_SIZE_BYTES
          }
        }
      }
    }
    whenever(vidLabelingJobService.batchCreateVidLabelingJobs(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<BatchCreateVidLabelingJobsRequest>(0)
      batchCreateVidLabelingJobsResponse {
        for ((i, createRequest) in request.requestsList.withIndex()) {
          vidLabelingJobs +=
            createRequest.vidLabelingJob.copy { name = "${request.parent}/vidLabelingJobs/job-$i" }
        }
      }
    }
  }

  /** Stubs `getModelLine` to return a model line with the fixed active window. */
  private suspend fun stubModelLine() {
    whenever(modelLinesService.getModelLine(any()))
      .thenReturn(
        modelLine {
          name = MODEL_LINE
          activeStartTime = ACTIVE_START_TIME
          activeEndTime = ACTIVE_END_TIME
        }
      )
  }

  /**
   * Stubs `batchCreatePoolAssignmentJobs` to echo back one `PoolAssignmentJob` per requested shard,
   * each with a deterministic resource name embedding its shard index.
   */
  private suspend fun stubPoolAssignmentJobs() {
    whenever(poolAssignmentJobService.batchCreatePoolAssignmentJobs(any())).thenAnswer { invocation
      ->
      val request = invocation.getArgument<BatchCreatePoolAssignmentJobsRequest>(0)
      batchCreatePoolAssignmentJobsResponse {
        for (createRequest in request.requestsList) {
          val shardIndex = createRequest.poolAssignmentJob.shardIndex
          poolAssignmentJobs += poolAssignmentJob {
            name = "${request.parent}/poolAssignmentJobs/job-$shardIndex"
            cmmsModelLine = createRequest.poolAssignmentJob.cmmsModelLine
            this.shardIndex = shardIndex
          }
        }
      }
    }
  }

  private suspend fun stubMarkPoolAssigning() {
    whenever(
        rawImpressionUploadModelLineService.markRawImpressionUploadModelLinePoolAssigning(any())
      )
      .thenReturn(rawImpressionUploadModelLine {})
  }

  /** Stubs all the RPCs a successful memoized dispatch needs. */
  private suspend fun stubMemoizedDispatch() {
    stubShardResolution(memoized = true)
    stubModelLine()
    stubPoolAssignmentJobs()
    whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
    stubMarkPoolAssigning()
  }

  private fun upload(
    id: String,
    state: RawImpressionUpload.State,
    createdAt: Instant,
    registrationComplete: Boolean = true,
    processingDeferred: Boolean = false,
    uploadHealingOperation: String = "",
    replacesRawImpressionUpload: String = "",
  ) = rawImpressionUpload {
    name = "$DATA_PROVIDER/rawImpressionUploads/$id"
    this.state = state
    createTime = Timestamps.fromMillis(createdAt.toEpochMilli())
    this.registrationComplete = registrationComplete
    this.processingDeferred = processingDeferred
    this.uploadHealingOperation = uploadHealingOperation
    this.replacesRawImpressionUpload = replacesRawImpressionUpload
  }

  private fun healingOperation(
    name: String,
    vararg steps: UploadHealingStep,
  ): UploadHealingOperation = uploadHealingOperation {
    this.name = name
    this.steps += steps.toList()
  }

  private fun healingStep(
    operationName: String,
    sequenceNumber: Long,
    source: String,
    predecessor: String = "",
    replacement: String = "",
    state: UploadHealingStep.State = UploadHealingStep.State.COMPLETE,
    action: RawImpressionUploadModelLine.RecoveryAction =
      RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY,
    recoveryTarget: Boolean = true,
    cmmsModelLine: String = MODEL_LINE,
    memoized: Boolean = true,
  ): UploadHealingStep = uploadHealingStep {
    name = "$operationName/uploadHealingSteps/$sequenceNumber"
    this.sequenceNumber = sequenceNumber
    sourceRawImpressionUpload = source
    recoveryPredecessorRawImpressionUpload = predecessor
    replacementRawImpressionUpload = replacement
    this.state = state
    recoveryAction = action
    this.recoveryTarget = recoveryTarget
    this.cmmsModelLine = cmmsModelLine
    this.memoized = memoized
  }

  private fun createdModelLine(id: String = "ml1") = rawImpressionUploadModelLine {
    name = "$DATA_PROVIDER/rawImpressionUploads/upload-1/modelLines/$id"
    cmmsModelLine = MODEL_LINE
    state = RawImpressionUploadModelLine.State.CREATED
    etag = ETAG
  }

  @Test
  fun `dispatchNext dispatches the oldest CREATED upload when none are ACTIVE`() = runBlocking {
    stubUploads(created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW)))
    stubModelLines(createdModelLine())
    stubShardResolution(memoized = false)
    stubModelLine()
    stubNonMemoizedFilesAndJobs()
    whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
    stubMarkTransitions()
    val recordingThrottlers = VidLabelingRpcThrottlersTestHelper.recording()

    val result = createSequencer(rpcThrottlers = recordingThrottlers.throttlers).dispatchNext()

    assertThat(result.dispatchedUpload).isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1")
    // Non-memoized line: one WorkItem per shard, then MarkLabeling once.
    verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(any()) }
    verifyBlocking(rawImpressionUploadModelLineService) {
      markRawImpressionUploadModelLineLabeling(any())
    }
    assertThat(recordingThrottlers.kingdom.invocationCount).isEqualTo(3)
    assertThat(recordingThrottlers.metadataRead.invocationCount).isEqualTo(4)
    assertThat(recordingThrottlers.metadataWrite.invocationCount).isEqualTo(2)
    assertThat(recordingThrottlers.controlPlane.invocationCount).isEqualTo(2)
  }

  @Test
  fun `dispatchNext skips upload while registration is incomplete`() = runBlocking {
    stubUploads(
      created =
        listOf(
          upload(
            "upload-1",
            RawImpressionUpload.State.CREATED,
            FIXED_NOW,
            registrationComplete = false,
          )
        )
    )

    val result = createSequencer().dispatchNext()

    assertThat(result.dispatchedUpload).isNull()
    assertThat(result.queuedUploads).isEqualTo(0)
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      listRawImpressionUploadModelLines(any())
    }
    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
  }

  @Test
  fun `dispatchNext processes deferred uploads oldest first after fence release`() = runBlocking {
    val d4 =
      upload("upload-4", RawImpressionUpload.State.CREATED, FIXED_NOW, processingDeferred = true)
    val d5 =
      upload(
        "upload-5",
        RawImpressionUpload.State.CREATED,
        FIXED_NOW.plusSeconds(60),
        processingDeferred = true,
      )
    stubUploads(created = listOf(d4, d5))

    val sequencer = createSequencer()
    assertThat(sequencer.dispatchNext().dispatchedUpload).isNull()
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      listRawImpressionUploadModelLines(any())
    }

    val releasedD4 = d4.copy { processingDeferred = false }
    val releasedD5 = d5.copy { processingDeferred = false }
    stubUploads(created = listOf(releasedD4, releasedD5))
    stubModelLinesByParent(
      mapOf(
        d4.name to
          listOf(modelLine(d4.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED)),
        d5.name to
          listOf(modelLine(d5.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED)),
      )
    )
    stubMemoizedDispatch()

    val first = sequencer.dispatchNext()
    assertThat(first.dispatchedUpload).isEqualTo(d4.name)
    assertThat(first.queuedUploads).isEqualTo(1)

    stubUploads(created = listOf(releasedD5))
    stubModelLinesByParent(
      mapOf(
        d5.name to
          listOf(modelLine(d5.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED))
      )
    )
    val second = sequencer.dispatchNext()
    assertThat(second.dispatchedUpload).isEqualTo(d5.name)

    val captor =
      argumentCaptor<
        org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadModelLinePoolAssigningRequest
      >()
    verifyBlocking(rawImpressionUploadModelLineService, times(2)) {
      markRawImpressionUploadModelLinePoolAssigning(captor.capture())
    }
    assertThat(captor.allValues.map { it.name })
      .containsExactly("${d4.name}/modelLines/ml", "${d5.name}/modelLines/ml")
      .inOrder()
  }

  @Test
  fun `dispatchNext does not start a model line already running in another upload`() = runBlocking {
    val active = upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW.minusSeconds(60))
    val created = upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW)
    stubUploads(active = listOf(active), created = listOf(created))
    stubModelLinesByParent(
      mapOf(
        active.name to
          listOf(modelLine(active.name, MODEL_LINE, RawImpressionUploadModelLine.State.LABELING)),
        created.name to
          listOf(modelLine(created.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED)),
      )
    )

    val result = createSequencer().dispatchNext()

    // MODEL_LINE is already running on the active upload, so the created upload's instance waits.
    assertThat(result.dispatchedUpload).isNull()
    assertThat(result.queuedUploads).isEqualTo(1)
    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      markRawImpressionUploadModelLineLabeling(any())
    }
  }

  @Test
  fun `dispatchNext starts a different model line in parallel with a running one`() =
    runBlocking<Unit> {
      val active = upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW.minusSeconds(60))
      val created = upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW)
      stubUploads(active = listOf(active), created = listOf(created))
      stubModelLinesByParent(
        mapOf(
          active.name to
            listOf(modelLine(active.name, MODEL_LINE, RawImpressionUploadModelLine.State.LABELING)),
          created.name to
            listOf(
              modelLine(created.name, MODEL_LINE_2, RawImpressionUploadModelLine.State.CREATED)
            ),
        )
      )
      stubShardResolution(memoized = false)
      stubModelLine()
      stubNonMemoizedFilesAndJobs()
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      stubMarkTransitions()

      val result = createSequencer().dispatchNext()

      // MODEL_LINE_2 differs from the running MODEL_LINE, so it dispatches in parallel.
      assertThat(result.dispatchedUpload).isEqualTo(created.name)
      verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS * 2)) { createWorkItem(any()) }
      verifyBlocking(rawImpressionUploadModelLineService) {
        markRawImpressionUploadModelLineLabeling(any())
      }
    }

  @Test
  fun `dispatchNext returns null when no uploads are CREATED`() = runBlocking {
    stubUploads()

    val result = createSequencer().dispatchNext()

    assertThat(result.dispatchedUpload).isNull()
    assertThat(result.queuedUploads).isEqualTo(0)
  }

  @Test
  fun `dispatchNext dispatches a memoized model line to Phase-0`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubMemoizedDispatch()
      val recordingThrottlers = VidLabelingRpcThrottlersTestHelper.recording()

      val result = createSequencer(rpcThrottlers = recordingThrottlers.throttlers).dispatchNext()

      assertThat(result.dispatchedUpload).isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1")
      // Memoized line: one PoolAssignmentJob batch, one SubpoolAssigner WorkItem per shard, then
      // MarkPoolAssigning once. The non-memoized MarkLabeling must NOT be called.
      verifyBlocking(poolAssignmentJobService) { batchCreatePoolAssignmentJobs(any()) }
      verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(any()) }
      verifyBlocking(rawImpressionUploadModelLineService) {
        markRawImpressionUploadModelLinePoolAssigning(any())
      }
      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        markRawImpressionUploadModelLineLabeling(any())
      }
      assertThat(recordingThrottlers.kingdom.invocationCount).isEqualTo(3)
      assertThat(recordingThrottlers.metadataRead.invocationCount).isEqualTo(3)
      assertThat(recordingThrottlers.metadataWrite.invocationCount).isEqualTo(2)
      assertThat(recordingThrottlers.controlPlane.invocationCount).isEqualTo(2)
      val dispatchSpan =
        spanExporter.finishedSpanItems.single { it.name == "edpa.vid_labeling.dispatch" }
      assertThat(dispatchSpan.attributes.get(VidLabelingTraceAttributes.PIPELINE_PHASE))
        .isEqualTo("phase0")
      assertThat(dispatchSpan.attributes.get(XmmTraceAttributes.OUTCOME)).isEqualTo("succeeded")
      assertThat(dispatchSpan.events.map { it.name })
        .containsAtLeast(
          "edpa.vid_labeling.dispatch.job",
          "edpa.vid_labeling.dispatch.work_item",
          "edpa.vid_labeling.dispatch.transition",
        )
      val jobEvents = dispatchSpan.events.filter { it.name == "edpa.vid_labeling.dispatch.job" }
      assertThat(
          jobEvents.mapNotNull {
            it.attributes.get(VidLabelingTraceAttributes.POOL_ASSIGNMENT_JOB_NAME)
          }
        )
        .hasSize(NUMBER_OF_SHARDS)
      assertThat(jobEvents.map { it.attributes.get(XmmTraceAttributes.OUTCOME) }.toSet())
        .containsExactly("resolved")
      val workItemEvents =
        dispatchSpan.events.filter { it.name == "edpa.vid_labeling.dispatch.work_item" }
      assertThat(workItemEvents.mapNotNull { it.attributes.get(XmmTraceAttributes.WORK_ITEM_NAME) })
        .hasSize(NUMBER_OF_SHARDS)
      val transition =
        dispatchSpan.events.single { it.name == "edpa.vid_labeling.dispatch.transition" }
      assertThat(
          transition.attributes.get(
            VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_MODEL_LINE_NAME
          )
        )
        .isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1/modelLines/ml1")
      assertThat(transition.attributes.get(XmmTraceAttributes.OUTCOME)).isEqualTo("pool_assigning")
      val jobLogs =
        logRecords.filter { it.message.contains("event=edpa.vid_labeling.dispatch.job ") }
      assertThat(jobLogs).hasSize(NUMBER_OF_SHARDS)
      jobLogs.forEach {
        assertThat(it.message).contains("xmm.lifecycle.stage=dispatch")
        assertThat(it.message).contains("xmm.outcome=resolved")
      }
      val workItemLogs =
        logRecords.filter { it.message.contains("event=edpa.vid_labeling.dispatch.work_item ") }
      assertThat(workItemLogs).hasSize(NUMBER_OF_SHARDS)
      workItemLogs.forEach { assertThat(it.message).contains("xmm.work_item.name=workItems/") }
    }

  @Test
  fun `dispatchNext threads each shard's PoolAssignmentJob and active window into its WorkItem`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubMemoizedDispatch()

      createSequencer(traceContextProvider = { TRACE_CONTEXT }).dispatchNext()

      val captor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(captor.capture()) }
      // Each WorkItem targets the Phase-0 queue and carries a SubpoolAssignerParams whose
      // pool_assignment_job matches its own shard index, plus the model line's active window.
      val paramsByShard: Map<Int, SubpoolAssignerParams> =
        captor.allValues.associate { request ->
          assertThat(request.workItem.queue).isEqualTo(POOL_ASSIGNER_QUEUE_NAME)
          val workItemParams = request.workItem.workItemParams.unpack<WorkItemParams>()
          assertThat(workItemParams.traceContextMap).containsExactlyEntriesIn(TRACE_CONTEXT)
          val params = workItemParams.appParams.unpack<SubpoolAssignerParams>()
          params.shardIndex to params
        }
      assertThat(paramsByShard.keys).containsExactly(0, 1)
      for ((shardIndex, params) in paramsByShard) {
        assertThat(params.poolAssignmentJob)
          .isEqualTo(
            "$DATA_PROVIDER/rawImpressionUploads/upload-1/poolAssignmentJobs/job-$shardIndex"
          )
        assertThat(params.modelLine).isEqualTo(MODEL_LINE)
        assertThat(params.totalShards).isEqualTo(NUMBER_OF_SHARDS)
        assertThat(params.rawImpressionUpload)
          .isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1")
        assertThat(params.activeStartTime).isEqualTo(ACTIVE_START_TIME)
        assertThat(params.activeEndTime).isEqualTo(ACTIVE_END_TIME)
        assertThat(params.modelStorageParams.blobPrefix).isEqualTo("gs://model-bucket")
        // The bin-packing threshold is forwarded onto SubpoolAssignerParams so the memoized Phase-1
        // fan-out bin-packs the same way the non-memoized dispatcher does (SubpoolAssignerApp
        // requires it > 0).
        assertThat(params.maxFileBatchSizeBytes).isEqualTo(MAX_FILE_BATCH_SIZE_BYTES)
        // The event-template descriptor + type pass through to SubpoolAssignerParams so the
        // memoized Phase-1 last-out can stamp them on the Phase-2 VidLabeler ModelLineConfig.
        assertThat(params.eventTemplateDescriptorBlobUri)
          .isEqualTo(EVENT_TEMPLATE_DESCRIPTOR_BLOB_URI)
        assertThat(params.eventTemplateType).isEqualTo(EVENT_TEMPLATE_TYPE)
        // Same for the PopulationSpec: Phase-2 rejects a WorkItem without it, so dropping it here
        // would strand the memoized fan-out on a crash-looping VidLabeler.
        assertThat(params.populationSpecBlobUri).isEqualTo(POPULATION_SPEC_BLOB_URI)
      }
    }

  @Test
  fun `dispatchNext follows healing steps when upload create times are inverse`() =
    runBlocking<Unit> {
      val operationName = "$DATA_PROVIDER/uploadHealingOperations/healing"
      val d1 = upload("day-1", RawImpressionUpload.State.COMPLETED, FIXED_NOW.minusSeconds(1))
      val sourceD2 = upload("day-2", RawImpressionUpload.State.FAILED, FIXED_NOW)
      val sourceD3 = upload("day-3", RawImpressionUpload.State.FAILED, FIXED_NOW.plusSeconds(1))
      val sourceD4 = upload("day-4", RawImpressionUpload.State.FAILED, FIXED_NOW.plusSeconds(2))
      val correctedD2 =
        upload(
          "corrected-day-2",
          RawImpressionUpload.State.ACTIVE,
          FIXED_NOW.plusSeconds(4),
          uploadHealingOperation = operationName,
          replacesRawImpressionUpload = sourceD2.name,
        )
      val rebuiltD3 =
        upload(
          "rebuilt-day-3",
          RawImpressionUpload.State.ACTIVE,
          FIXED_NOW.plusSeconds(5),
          uploadHealingOperation = operationName,
          replacesRawImpressionUpload = sourceD3.name,
        )
      val correctedD4 =
        upload(
          "corrected-day-4",
          RawImpressionUpload.State.CREATED,
          FIXED_NOW.plusSeconds(3),
          uploadHealingOperation = operationName,
          replacesRawImpressionUpload = sourceD4.name,
        )
      stubUploads(active = listOf(correctedD2, rebuiltD3), created = listOf(correctedD4))
      stubGetUploads(sourceD2, sourceD3, sourceD4)
      stubModelLinesByParent(
        mapOf(
          d1.name to
            listOf(modelLine(d1.name, MODEL_LINE, RawImpressionUploadModelLine.State.COMPLETED)),
          correctedD2.name to
            listOf(
              modelLine(
                correctedD2.name,
                MODEL_LINE,
                RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
              )
            ),
          rebuiltD3.name to
            listOf(
              modelLine(
                rebuiltD3.name,
                MODEL_LINE,
                RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
              )
            ),
          correctedD4.name to
            listOf(
              modelLine(correctedD4.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED)
            ),
        )
      )
      stubHealingOperation(
        healingOperation(
          operationName,
          healingStep(operationName, 1, sourceD2.name, d1.name, correctedD2.name),
          healingStep(operationName, 2, sourceD3.name, sourceD2.name, rebuiltD3.name),
          healingStep(
            operationName,
            3,
            sourceD4.name,
            sourceD3.name,
            state = UploadHealingStep.State.RECOVERY_STARTED,
            action = RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION,
          ),
        )
      )
      stubLiveSnapshots(
        d1.name to MODEL_LINE,
        correctedD2.name to MODEL_LINE,
        rebuiltD3.name to MODEL_LINE,
      )
      stubMemoizedDispatch()

      createSequencer().dispatchNext()

      val captor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(captor.capture()) }
      for (request in captor.allValues) {
        val params =
          request.workItem.workItemParams
            .unpack<WorkItemParams>()
            .appParams
            .unpack<SubpoolAssignerParams>()
        assertThat(params.allowedPriorSnapshotRawImpressionUploadsList)
          .containsExactly(correctedD2.name, rebuiltD3.name)
          .inOrder()
      }
      verifyBlocking(uploadHealingOperationService, times(1)) { getUploadHealingOperation(any()) }
    }

  @Test
  fun `dispatchNext blocks a physically earlier future healing handoff`() = runBlocking {
    val operationName = "$DATA_PROVIDER/uploadHealingOperations/healing"
    val d1 = upload("day-1", RawImpressionUpload.State.COMPLETED, FIXED_NOW.minusSeconds(1))
    val sourceD3 = upload("day-3", RawImpressionUpload.State.FAILED, FIXED_NOW)
    val sourceD4 = upload("day-4", RawImpressionUpload.State.FAILED, FIXED_NOW.plusSeconds(1))
    val sourceD5 = upload("day-5", RawImpressionUpload.State.FAILED, FIXED_NOW.plusSeconds(2))
    val rebuiltD3 =
      upload(
        "rebuilt-day-3",
        RawImpressionUpload.State.ACTIVE,
        FIXED_NOW.plusSeconds(4),
        uploadHealingOperation = operationName,
        replacesRawImpressionUpload = sourceD3.name,
      )
    val futureD5 =
      upload(
        "rebuilt-day-5",
        RawImpressionUpload.State.ACTIVE,
        FIXED_NOW.plusSeconds(3),
        uploadHealingOperation = operationName,
        replacesRawImpressionUpload = sourceD5.name,
      )
    val correctedD4 =
      upload(
        "corrected-day-4",
        RawImpressionUpload.State.CREATED,
        FIXED_NOW.plusSeconds(5),
        uploadHealingOperation = operationName,
        replacesRawImpressionUpload = sourceD4.name,
      )
    stubUploads(active = listOf(rebuiltD3, futureD5), created = listOf(correctedD4))
    stubGetUploads(sourceD3, sourceD4)
    stubModelLinesByParent(
      mapOf(
        d1.name to
          listOf(modelLine(d1.name, MODEL_LINE, RawImpressionUploadModelLine.State.COMPLETED)),
        rebuiltD3.name to
          listOf(
            modelLine(
              rebuiltD3.name,
              MODEL_LINE,
              RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
            )
          ),
        futureD5.name to
          listOf(
            modelLine(
              futureD5.name,
              MODEL_LINE,
              RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
            )
          ),
        correctedD4.name to
          listOf(
            modelLine(correctedD4.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED)
          ),
      )
    )
    stubHealingOperation(
      healingOperation(
        operationName,
        healingStep(operationName, 2, sourceD3.name, d1.name, rebuiltD3.name),
        healingStep(
          operationName,
          3,
          sourceD4.name,
          sourceD3.name,
          state = UploadHealingStep.State.RECOVERY_STARTED,
        ),
        healingStep(operationName, 4, sourceD5.name, sourceD4.name, futureD5.name),
      )
    )
    stubLiveSnapshots(d1.name to MODEL_LINE, rebuiltD3.name to MODEL_LINE)

    val result = createSequencer().dispatchNext()

    assertThat(result.dispatchedUpload).isNull()
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      markRawImpressionUploadModelLinePoolAssigning(any())
    }
  }

  @Test
  fun `dispatchNext fails closed on ambiguous recovery target lineage`() = runBlocking {
    val operationName = "$DATA_PROVIDER/uploadHealingOperations/healing"
    val olderSource = upload("day-4-v1", RawImpressionUpload.State.FAILED, FIXED_NOW)
    val source =
      upload(
        "day-4-v2",
        RawImpressionUpload.State.FAILED,
        FIXED_NOW.plusSeconds(1),
        replacesRawImpressionUpload = olderSource.name,
      )
    val candidate =
      upload(
        "day-4-v3",
        RawImpressionUpload.State.CREATED,
        FIXED_NOW.plusSeconds(2),
        uploadHealingOperation = operationName,
        replacesRawImpressionUpload = source.name,
      )
    stubUploads(created = listOf(candidate))
    stubGetUploads(source, olderSource)
    stubModelLinesByParent(
      mapOf(
        candidate.name to
          listOf(modelLine(candidate.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED))
      )
    )
    stubHealingOperation(
      healingOperation(
        operationName,
        healingStep(
          operationName,
          1,
          olderSource.name,
          state = UploadHealingStep.State.RECOVERY_STARTED,
        ),
        healingStep(operationName, 2, source.name, state = UploadHealingStep.State.RECOVERY_STARTED),
      )
    )

    val exception = assertFailsWith<IllegalStateException> { createSequencer().dispatchNext() }

    assertThat(exception).hasMessageThat().contains("Expected one recovery target")
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      markRawImpressionUploadModelLinePoolAssigning(any())
    }
  }

  @Test
  fun `dispatchNext fails closed when predecessor replacement is missing`() = runBlocking {
    val operationName = "$DATA_PROVIDER/uploadHealingOperations/healing"
    val sourceD3 = upload("day-3", RawImpressionUpload.State.FAILED, FIXED_NOW)
    val sourceD4 = upload("day-4", RawImpressionUpload.State.FAILED, FIXED_NOW.plusSeconds(1))
    val candidate =
      upload(
        "corrected-day-4",
        RawImpressionUpload.State.CREATED,
        FIXED_NOW.plusSeconds(2),
        uploadHealingOperation = operationName,
        replacesRawImpressionUpload = sourceD4.name,
      )
    stubUploads(created = listOf(candidate))
    stubGetUploads(sourceD4)
    stubModelLinesByParent(
      mapOf(
        candidate.name to
          listOf(modelLine(candidate.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED))
      )
    )
    stubHealingOperation(
      healingOperation(
        operationName,
        healingStep(operationName, 2, sourceD3.name),
        healingStep(
          operationName,
          3,
          sourceD4.name,
          sourceD3.name,
          state = UploadHealingStep.State.RECOVERY_STARTED,
        ),
      )
    )

    val exception = assertFailsWith<IllegalStateException> { createSequencer().dispatchNext() }

    assertThat(exception).hasMessageThat().contains("has no replacement")
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      markRawImpressionUploadModelLinePoolAssigning(any())
    }
  }

  @Test
  fun `dispatchNext fails closed when predecessor replacement has no live snapshot`() =
    runBlocking<Unit> {
      val operationName = "$DATA_PROVIDER/uploadHealingOperations/healing"
      val sourceD3 = upload("day-3", RawImpressionUpload.State.FAILED, FIXED_NOW)
      val sourceD4 = upload("day-4", RawImpressionUpload.State.FAILED, FIXED_NOW.plusSeconds(1))
      val rebuiltD3 =
        upload(
          "rebuilt-day-3",
          RawImpressionUpload.State.ACTIVE,
          FIXED_NOW.plusSeconds(3),
          uploadHealingOperation = operationName,
          replacesRawImpressionUpload = sourceD3.name,
        )
      val candidate =
        upload(
          "corrected-day-4",
          RawImpressionUpload.State.CREATED,
          FIXED_NOW.plusSeconds(2),
          uploadHealingOperation = operationName,
          replacesRawImpressionUpload = sourceD4.name,
        )
      stubUploads(active = listOf(rebuiltD3), created = listOf(candidate))
      stubGetUploads(sourceD3, sourceD4)
      stubModelLinesByParent(
        mapOf(
          rebuiltD3.name to
            listOf(
              modelLine(
                rebuiltD3.name,
                MODEL_LINE,
                RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
              )
            ),
          candidate.name to
            listOf(
              modelLine(candidate.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED)
            ),
        )
      )
      stubHealingOperation(
        healingOperation(
          operationName,
          healingStep(operationName, 2, sourceD3.name, replacement = rebuiltD3.name),
          healingStep(
            operationName,
            3,
            sourceD4.name,
            sourceD3.name,
            state = UploadHealingStep.State.RECOVERY_STARTED,
          ),
        )
      )
      stubLiveSnapshots()

      val exception = assertFailsWith<IllegalStateException> { createSequencer().dispatchNext() }

      assertThat(exception).hasMessageThat().contains("has no live snapshot")
      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        markRawImpressionUploadModelLinePoolAssigning(any())
      }
    }

  @Test
  fun `dispatchNext fails closed when predecessor step is physically in the future`() =
    runBlocking<Unit> {
      val operationName = "$DATA_PROVIDER/uploadHealingOperations/healing"
      val sourceD3 = upload("day-3", RawImpressionUpload.State.FAILED, FIXED_NOW)
      val sourceD4 = upload("day-4", RawImpressionUpload.State.FAILED, FIXED_NOW.plusSeconds(1))
      val candidate =
        upload(
          "corrected-day-4",
          RawImpressionUpload.State.CREATED,
          FIXED_NOW.plusSeconds(2),
          uploadHealingOperation = operationName,
          replacesRawImpressionUpload = sourceD4.name,
        )
      stubUploads(created = listOf(candidate))
      stubGetUploads(sourceD4)
      stubModelLinesByParent(
        mapOf(
          candidate.name to
            listOf(
              modelLine(candidate.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED)
            )
        )
      )
      stubHealingOperation(
        healingOperation(
          operationName,
          healingStep(operationName, 4, sourceD3.name, replacement = "replacement"),
          healingStep(
            operationName,
            3,
            sourceD4.name,
            sourceD3.name,
            state = UploadHealingStep.State.RECOVERY_STARTED,
          ),
        )
      )

      val exception = assertFailsWith<IllegalStateException> { createSequencer().dispatchNext() }

      assertThat(exception).hasMessageThat().contains("is not earlier")
      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        markRawImpressionUploadModelLinePoolAssigning(any())
      }
    }

  @Test
  fun `dispatchNext does not authorize a cross-operation handoff`() = runBlocking {
    val operationName = "$DATA_PROVIDER/uploadHealingOperations/healing"
    val otherOperation = "$DATA_PROVIDER/uploadHealingOperations/other"
    val d1 = upload("day-1", RawImpressionUpload.State.COMPLETED, FIXED_NOW)
    val sourceD4 = upload("day-4", RawImpressionUpload.State.FAILED, FIXED_NOW.plusSeconds(1))
    val other =
      upload(
        "other",
        RawImpressionUpload.State.ACTIVE,
        FIXED_NOW.plusSeconds(2),
        uploadHealingOperation = otherOperation,
      )
    val candidate =
      upload(
        "corrected-day-4",
        RawImpressionUpload.State.CREATED,
        FIXED_NOW.plusSeconds(3),
        uploadHealingOperation = operationName,
        replacesRawImpressionUpload = sourceD4.name,
      )
    stubUploads(active = listOf(other), created = listOf(candidate))
    stubGetUploads(sourceD4)
    stubModelLinesByParent(
      mapOf(
        d1.name to
          listOf(modelLine(d1.name, MODEL_LINE, RawImpressionUploadModelLine.State.COMPLETED)),
        other.name to
          listOf(
            modelLine(
              other.name,
              MODEL_LINE,
              RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
            )
          ),
        candidate.name to
          listOf(modelLine(candidate.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED)),
      )
    )
    stubHealingOperation(
      healingOperation(
        operationName,
        healingStep(
          operationName,
          3,
          sourceD4.name,
          d1.name,
          state = UploadHealingStep.State.RECOVERY_STARTED,
        ),
      )
    )
    stubLiveSnapshots(d1.name to MODEL_LINE)

    val result = createSequencer().dispatchNext()

    assertThat(result.dispatchedUpload).isNull()
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      markRawImpressionUploadModelLinePoolAssigning(any())
    }
  }

  @Test
  fun `dispatchNext rejects an external predecessor that is not completed`() = runBlocking {
    val operationName = "$DATA_PROVIDER/uploadHealingOperations/healing"
    val d1 = upload("day-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)
    val sourceD4 = upload("day-4", RawImpressionUpload.State.FAILED, FIXED_NOW.plusSeconds(1))
    val candidate =
      upload(
        "corrected-day-4",
        RawImpressionUpload.State.CREATED,
        FIXED_NOW.plusSeconds(2),
        uploadHealingOperation = operationName,
        replacesRawImpressionUpload = sourceD4.name,
      )
    stubUploads(created = listOf(candidate))
    stubGetUploads(sourceD4)
    stubModelLinesByParent(
      mapOf(
        d1.name to
          listOf(
            modelLine(d1.name, MODEL_LINE, RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
          ),
        candidate.name to
          listOf(modelLine(candidate.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED)),
      )
    )
    stubHealingOperation(
      healingOperation(
        operationName,
        healingStep(
          operationName,
          3,
          sourceD4.name,
          d1.name,
          state = UploadHealingStep.State.RECOVERY_STARTED,
        ),
      )
    )

    val exception = assertFailsWith<IllegalStateException> { createSequencer().dispatchNext() }

    assertThat(exception).hasMessageThat().contains("is not COMPLETED")
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      markRawImpressionUploadModelLinePoolAssigning(any())
    }
  }

  @Test
  fun `dispatchNext fetches one healing operation per scan`() = runBlocking {
    val operationName = "$DATA_PROVIDER/uploadHealingOperations/healing"
    val d1 = upload("day-1", RawImpressionUpload.State.COMPLETED, FIXED_NOW)
    val sourceD4 = upload("day-4", RawImpressionUpload.State.FAILED, FIXED_NOW.plusSeconds(1))
    val candidate =
      upload(
        "corrected-day-4",
        RawImpressionUpload.State.CREATED,
        FIXED_NOW.plusSeconds(2),
        uploadHealingOperation = operationName,
        replacesRawImpressionUpload = sourceD4.name,
      )
    stubUploads(created = listOf(candidate))
    stubGetUploads(sourceD4)
    stubModelLinesByParent(
      mapOf(
        d1.name to
          listOf(
            modelLine(d1.name, MODEL_LINE, RawImpressionUploadModelLine.State.COMPLETED),
            modelLine(
              d1.name,
              MODEL_LINE_2,
              RawImpressionUploadModelLine.State.COMPLETED,
              id = "ml2",
            ),
          ),
        candidate.name to
          listOf(
            modelLine(candidate.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED),
            modelLine(
              candidate.name,
              MODEL_LINE_2,
              RawImpressionUploadModelLine.State.CREATED,
              id = "ml2",
            ),
          ),
      )
    )
    stubHealingOperation(
      healingOperation(
        operationName,
        healingStep(
          operationName,
          3,
          sourceD4.name,
          d1.name,
          state = UploadHealingStep.State.RECOVERY_STARTED,
        ),
        healingStep(
          operationName,
          4,
          sourceD4.name,
          d1.name,
          state = UploadHealingStep.State.RECOVERY_STARTED,
          cmmsModelLine = MODEL_LINE_2,
        ),
      )
    )
    stubLiveSnapshots(d1.name to MODEL_LINE, d1.name to MODEL_LINE_2)
    stubMemoizedDispatch()

    createSequencer().dispatchNext()

    verifyBlocking(uploadHealingOperationService, times(1)) { getUploadHealingOperation(any()) }
    verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS * 2)) { createWorkItem(any()) }
  }

  @Test
  fun `dispatchNext skips no-replacement step in persisted predecessor chain`() =
    runBlocking<Unit> {
      val operationName = "$DATA_PROVIDER/uploadHealingOperations/healing"
      val d1 = upload("day-1", RawImpressionUpload.State.COMPLETED, FIXED_NOW)
      val sourceD3 = upload("day-3", RawImpressionUpload.State.FAILED, FIXED_NOW.plusSeconds(1))
      val sourceD4 = upload("day-4", RawImpressionUpload.State.FAILED, FIXED_NOW.plusSeconds(2))
      val sourceD5 = upload("day-5", RawImpressionUpload.State.FAILED, FIXED_NOW.plusSeconds(3))
      val rebuiltD3 =
        upload(
          "rebuilt-day-3",
          RawImpressionUpload.State.ACTIVE,
          FIXED_NOW.plusSeconds(5),
          uploadHealingOperation = operationName,
          replacesRawImpressionUpload = sourceD3.name,
        )
      val rebuiltD5 =
        upload(
          "rebuilt-day-5",
          RawImpressionUpload.State.CREATED,
          FIXED_NOW.plusSeconds(4),
          uploadHealingOperation = operationName,
          replacesRawImpressionUpload = sourceD5.name,
        )
      stubUploads(active = listOf(rebuiltD3), created = listOf(rebuiltD5))
      stubGetUploads(sourceD3, sourceD5)
      stubModelLinesByParent(
        mapOf(
          d1.name to
            listOf(modelLine(d1.name, MODEL_LINE, RawImpressionUploadModelLine.State.COMPLETED)),
          rebuiltD3.name to
            listOf(
              modelLine(
                rebuiltD3.name,
                MODEL_LINE,
                RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
              )
            ),
          rebuiltD5.name to
            listOf(
              modelLine(rebuiltD5.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED)
            ),
        )
      )
      stubHealingOperation(
        healingOperation(
          operationName,
          healingStep(operationName, 2, sourceD3.name, d1.name, rebuiltD3.name),
          healingStep(
            operationName,
            3,
            sourceD4.name,
            sourceD3.name,
            action = RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_NO_REPLACEMENT,
            recoveryTarget = false,
          ),
          healingStep(
            operationName,
            4,
            sourceD5.name,
            sourceD4.name,
            state = UploadHealingStep.State.RECOVERY_STARTED,
          ),
        )
      )
      stubLiveSnapshots(d1.name to MODEL_LINE, rebuiltD3.name to MODEL_LINE)
      stubMemoizedDispatch()

      createSequencer().dispatchNext()

      val captor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(captor.capture()) }
      for (request in captor.allValues) {
        val params =
          request.workItem.workItemParams
            .unpack<WorkItemParams>()
            .appParams
            .unpack<SubpoolAssignerParams>()
        assertThat(params.allowedPriorSnapshotRawImpressionUploadsList)
          .containsExactly(rebuiltD3.name)
      }
    }

  @Test
  fun `dispatchNext creates a PoolAssignmentJob per shard with deterministic request ids`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubMemoizedDispatch()

      createSequencer().dispatchNext()

      val captor = argumentCaptor<BatchCreatePoolAssignmentJobsRequest>()
      verifyBlocking(poolAssignmentJobService) { batchCreatePoolAssignmentJobs(captor.capture()) }
      val batch = captor.firstValue
      assertThat(batch.requestsList.map { it.poolAssignmentJob.shardIndex }).containsExactly(0, 1)
      // Deterministic, shard-scoped request_ids so a redelivery is idempotent per shard.
      assertThat(batch.requestsList.map { it.requestId })
        .containsExactly(
          RequestIds.forPoolAssignmentJob(
            "$DATA_PROVIDER/rawImpressionUploads/upload-1",
            MODEL_LINE,
            0,
          ),
          RequestIds.forPoolAssignmentJob(
            "$DATA_PROVIDER/rawImpressionUploads/upload-1",
            MODEL_LINE,
            1,
          ),
        )
    }

  @Test
  fun `dispatchNext passes the model line etag on the PoolAssigning mark`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubMemoizedDispatch()

      createSequencer().dispatchNext()

      val captor = argumentCaptor<MarkRawImpressionUploadModelLinePoolAssigningRequest>()
      verifyBlocking(rawImpressionUploadModelLineService) {
        markRawImpressionUploadModelLinePoolAssigning(captor.capture())
      }
      assertThat(captor.firstValue.etag).isEqualTo(ETAG)
    }

  @Test
  fun `dispatchNext records shard identities when PoolAssignmentJob creation fails`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = true)
      stubModelLine()
      whenever(poolAssignmentJobService.batchCreatePoolAssignmentJobs(any())).thenAnswer {
        throw StatusException(Status.UNAVAILABLE.withDescription("metadata unavailable"))
      }

      assertFailsWith<StatusException> { createSequencer().dispatchNext() }

      val failureLogs =
        logRecords.filter {
          it.message.contains("event=edpa.vid_labeling.dispatch.job ") &&
            it.message.contains("xmm.outcome=failed")
        }
      assertThat(failureLogs).hasSize(NUMBER_OF_SHARDS)
      assertThat(failureLogs.any { it.message.contains("xmm.edpa.shard_index=0") }).isTrue()
      assertThat(failureLogs.any { it.message.contains("xmm.edpa.shard_index=1") }).isTrue()
    }

  @Test
  fun `dispatchNext tolerates an already-existing SubpoolAssigner WorkItem`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = true)
      stubModelLine()
      stubPoolAssignmentJobs()
      // A concurrent dispatch already published the WorkItem with the same deterministic ID.
      whenever(workItemsService.createWorkItem(any())).thenAnswer {
        throw StatusException(Status.ALREADY_EXISTS.withDescription("work item exists"))
      }
      stubMarkPoolAssigning()

      val result = createSequencer(traceContextProvider = { TRACE_CONTEXT }).dispatchNext()

      // Idempotent: the upload is still dispatched and the transition still happens.
      assertThat(result.dispatchedUpload).isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1")
      verifyBlocking(rawImpressionUploadModelLineService) {
        markRawImpressionUploadModelLinePoolAssigning(any())
      }
    }

  @Test
  fun `dispatchNext records a failed SubpoolAssigner WorkItem identifier`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = true)
      stubModelLine()
      stubPoolAssignmentJobs()
      whenever(workItemsService.createWorkItem(any())).thenAnswer {
        throw StatusException(Status.UNAVAILABLE.withDescription("control plane unavailable"))
      }

      assertFailsWith<StatusException> { createSequencer().dispatchNext() }

      val failureLog =
        logRecords.single {
          it.message.contains("event=edpa.vid_labeling.dispatch.work_item ") &&
            it.message.contains("xmm.outcome=failed")
        }
      assertThat(failureLog.message).contains("xmm.work_item.name=workItems/")
      assertThat(failureLog.message).contains("xmm.lifecycle.stage=dispatch")
      assertThat(failureLog.message).contains("xmm.error.code=grpc.UNAVAILABLE")
    }

  @Test
  fun `dispatchNext skips a memoized model line whose claim was lost with ABORTED`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = true)
      stubModelLine()
      stubPoolAssignmentJobs()
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      // Another concurrent dispatcher claimed the model line first: the etag CAS fails with
      // ABORTED.
      whenever(
          rawImpressionUploadModelLineService.markRawImpressionUploadModelLinePoolAssigning(any())
        )
        .thenAnswer { throw StatusException(Status.ABORTED.withDescription("etag mismatch")) }
      whenever(rawImpressionUploadModelLineService.getRawImpressionUploadModelLine(any()))
        .thenReturn(
          createdModelLine().copy { state = RawImpressionUploadModelLine.State.POOL_ASSIGNING }
        )

      // dispatchNext must not propagate the lost-race error.
      val result = createSequencer().dispatchNext()

      assertThat(result.dispatchedUpload).isNull()
    }

  @Test
  fun `dispatchNext publishes no memoized work when a correction fence rejects the claim`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = true)
      stubModelLine()
      stubPoolAssignmentJobs()
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      whenever(
          rawImpressionUploadModelLineService.markRawImpressionUploadModelLinePoolAssigning(any())
        )
        .thenAnswer {
          throw StatusException(
            Status.FAILED_PRECONDITION.withDescription("another upload owns the model line")
          )
        }

      val result = createSequencer().dispatchNext()

      assertThat(result.dispatchedUpload).isNull()
      verifyBlocking(rawImpressionUploadModelLineService, times(1)) {
        markRawImpressionUploadModelLinePoolAssigning(any())
      }
      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        getRawImpressionUploadModelLine(any())
      }
      verifyBlocking(poolAssignmentJobService, never()) { batchCreatePoolAssignmentJobs(any()) }
      verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
    }

  @Test
  fun `dispatchNext retries a memoized claim when only the parent etag changed`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = true)
      stubModelLine()
      stubPoolAssignmentJobs()
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      whenever(
          rawImpressionUploadModelLineService.markRawImpressionUploadModelLinePoolAssigning(any())
        )
        .thenAnswer { throw StatusException(Status.ABORTED.withDescription("etag mismatch")) }
        .thenReturn(
          createdModelLine().copy { state = RawImpressionUploadModelLine.State.POOL_ASSIGNING }
        )
      whenever(rawImpressionUploadModelLineService.getRawImpressionUploadModelLine(any()))
        .thenReturn(createdModelLine().copy { etag = "fresh-etag" })

      val result = createSequencer().dispatchNext()

      assertThat(result.dispatchedUpload).isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1")
      val requests = argumentCaptor<MarkRawImpressionUploadModelLinePoolAssigningRequest>()
      verifyBlocking(rawImpressionUploadModelLineService, times(2)) {
        markRawImpressionUploadModelLinePoolAssigning(requests.capture())
      }
      assertThat(requests.allValues.map { it.etag }).containsExactly(ETAG, "fresh-etag").inOrder()
    }

  @Test
  fun `dispatchNext chunks PoolAssignmentJob creation above the batch limit`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubMemoizedDispatch()

      // 120 shards -> 50 + 50 + 20 across three BatchCreate calls; one WorkItem per shard.
      createSequencer(numberOfShards = 120).dispatchNext()

      verifyBlocking(poolAssignmentJobService, times(3)) { batchCreatePoolAssignmentJobs(any()) }
      verifyBlocking(workItemsService, times(120)) { createWorkItem(any()) }
    }

  @Test
  fun `dispatchNext omits active_end_time for a model line with an open-ended window`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = true)
      stubPoolAssignmentJobs()
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      stubMarkPoolAssigning()
      // Model line with no active_end_time: an open-ended (unbounded) active window.
      whenever(modelLinesService.getModelLine(any()))
        .thenReturn(
          modelLine {
            name = MODEL_LINE
            activeStartTime = ACTIVE_START_TIME
          }
        )

      createSequencer().dispatchNext()

      val captor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(captor.capture()) }
      // Every shard's params carry active_start_time but leave active_end_time unset.
      for (request in captor.allValues) {
        val params =
          request.workItem.workItemParams
            .unpack<WorkItemParams>()
            .appParams
            .unpack<SubpoolAssignerParams>()
        assertThat(params.activeStartTime).isEqualTo(ACTIVE_START_TIME)
        assertThat(params.hasActiveEndTime()).isFalse()
      }
    }

  @Test
  fun `dispatchNext propagates a getModelLine failure for a memoized model line`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = true)
      whenever(modelLinesService.getModelLine(any())).thenAnswer {
        throw StatusException(Status.NOT_FOUND.withDescription("model line missing"))
      }

      val exception = assertFailsWith<StatusException> { createSequencer().dispatchNext() }

      // The gRPC Status.Code propagates intact (no opaque Exception wrap).
      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      // The model line is never transitioned when its active window cannot be resolved.
      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        markRawImpressionUploadModelLinePoolAssigning(any())
      }
    }

  @Test
  fun `dispatchNext fails fast when a memoized model line lacks vid_rank_map_storage_params`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = true)
      val templateMissingVidRankMap =
        SUBPOOL_ASSIGNER_PARAMS_TEMPLATE.copy { clearVidRankMapStorageParams() }

      val exception =
        assertFailsWith<Exception> {
          createSequencer(subpoolAssignerParamsTemplate = templateMissingVidRankMap).dispatchNext()
        }

      assertThat(exception).hasMessageThat().contains("vid_rank_map_storage_params missing")
      // Nothing is created or transitioned when a REQUIRED storage param is absent.
      verifyBlocking(poolAssignmentJobService, never()) { batchCreatePoolAssignmentJobs(any()) }
      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        markRawImpressionUploadModelLinePoolAssigning(any())
      }
    }

  @Test
  fun `dispatchNext fails fast when a memoized model line lacks subpool_map_storage_params`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = true)
      val templateMissingSubpoolMap =
        SUBPOOL_ASSIGNER_PARAMS_TEMPLATE.copy { clearSubpoolMapStorageParams() }

      val exception =
        assertFailsWith<Exception> {
          createSequencer(subpoolAssignerParamsTemplate = templateMissingSubpoolMap).dispatchNext()
        }

      assertThat(exception).hasMessageThat().contains("subpool_map_storage_params missing")
      verifyBlocking(poolAssignmentJobService, never()) { batchCreatePoolAssignmentJobs(any()) }
      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        markRawImpressionUploadModelLinePoolAssigning(any())
      }
    }

  @Test
  fun `dispatchNext fails fast when a memoized model line lacks model_storage_params`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = true)
      val templateMissingModelStorage =
        SUBPOOL_ASSIGNER_PARAMS_TEMPLATE.copy { clearModelStorageParams() }

      val exception =
        assertFailsWith<Exception> {
          createSequencer(subpoolAssignerParamsTemplate = templateMissingModelStorage)
            .dispatchNext()
        }

      assertThat(exception).hasMessageThat().contains("model_storage_params missing")
      verifyBlocking(poolAssignmentJobService, never()) { batchCreatePoolAssignmentJobs(any()) }
      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        markRawImpressionUploadModelLinePoolAssigning(any())
      }
    }

  @Test
  fun `dispatchNext fails fast when a memoized model line lacks max_file_batch_size_bytes`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = true)
      val templateMissingMaxFileBatchSize =
        SUBPOOL_ASSIGNER_PARAMS_TEMPLATE.copy { maxFileBatchSizeBytes = 0 }

      val exception =
        assertFailsWith<Exception> {
          createSequencer(subpoolAssignerParamsTemplate = templateMissingMaxFileBatchSize)
            .dispatchNext()
        }

      assertThat(exception).hasMessageThat().contains("max_file_batch_size_bytes missing")
      verifyBlocking(poolAssignmentJobService, never()) { batchCreatePoolAssignmentJobs(any()) }
      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        markRawImpressionUploadModelLinePoolAssigning(any())
      }
    }

  @Test
  fun `dispatchNext selects the oldest CREATED upload`() =
    runBlocking<Unit> {
      val older =
        upload("upload-old", RawImpressionUpload.State.CREATED, FIXED_NOW.minusSeconds(3600))
      val newer = upload("upload-new", RawImpressionUpload.State.CREATED, FIXED_NOW)
      stubUploads(created = listOf(newer, older))
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = false)
      stubModelLine()
      stubNonMemoizedFilesAndJobs()
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      stubMarkTransitions()

      val result = createSequencer(traceContextProvider = { TRACE_CONTEXT }).dispatchNext()

      assertThat(result.dispatchedUpload)
        .isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-old")
      // WorkItem IDs embed the dispatched upload id.
      val captor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(captor.capture()) }
      // One WorkItem per bin-packed VidLabelingJob; the ID is an RFC 1034 resource id
      // (a "vl-" letter-prefixed deterministic UUID), valid for any job resource id.
      val vlWorkItemIds = captor.allValues.map { it.workItemId }
      assertThat(vlWorkItemIds).hasSize(2)
      assertThat(vlWorkItemIds.toSet()).hasSize(2)
      vlWorkItemIds.forEach { assertThat(it).matches("vl-[0-9a-f-]{36}") }
      // The WorkItem's VidLabelerParams carries the RawImpressionUpload resource name + its job.
      val vidLabelerParams =
        captor.firstValue.workItem.workItemParams
          .unpack<WorkItemParams>()
          .appParams
          .unpack<VidLabelerParams>()
      assertThat(vidLabelerParams.rawImpressionUpload)
        .isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-old")
      assertThat(vidLabelerParams.vidLabelingJob)
        .isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-old/vidLabelingJobs/job-0")
      assertThat(captor.firstValue.workItem.workItemParams.unpack<WorkItemParams>().traceContextMap)
        .containsExactlyEntriesIn(TRACE_CONTEXT)
      val dispatchSpan =
        spanExporter.finishedSpanItems.single { it.name == "edpa.vid_labeling.dispatch" }
      assertThat(dispatchSpan.attributes.get(VidLabelingTraceAttributes.PIPELINE_PHASE))
        .isEqualTo("phase2")
      val phase2Jobs = dispatchSpan.events.filter { it.name == "edpa.vid_labeling.dispatch.job" }
      assertThat(
          phase2Jobs.mapNotNull {
            it.attributes.get(VidLabelingTraceAttributes.VID_LABELING_JOB_NAME)
          }
        )
        .isNotEmpty()
    }

  @Test
  fun `dispatchNext leaves a non-memoized line unclaimed when its file listing is empty`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = false)
      stubModelLine()
      whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
        .thenReturn(listRawImpressionUploadFilesResponse {})
      stubMarkTransitions()

      val result = createSequencer().dispatchNext()

      assertThat(result.dispatchedUpload).isNull()
      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        markRawImpressionUploadModelLineLabeling(any())
      }
      verifyBlocking(vidLabelingJobService, never()) { batchCreateVidLabelingJobs(any()) }
      verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
    }

  @Test
  fun `dispatchNext leaves a non-memoized line unclaimed when its file listing fails`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = false)
      stubModelLine()
      whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any())).thenAnswer {
        throw StatusException(Status.UNAVAILABLE)
      }
      stubMarkTransitions()

      assertFailsWith<StatusException> { createSequencer().dispatchNext() }

      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        markRawImpressionUploadModelLineLabeling(any())
      }
      verifyBlocking(vidLabelingJobService, never()) { batchCreateVidLabelingJobs(any()) }
      verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
    }

  @Test
  fun `dispatchNext retries an empty non-memoized file listing without an extra claim`() =
    runBlocking<Unit> {
      val created = upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW)
      stubUploads(created = listOf(created))
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = false)
      stubModelLine()
      whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
        .thenReturn(listRawImpressionUploadFilesResponse {})
      stubMarkTransitions()
      val sequencer = createSequencer()

      assertThat(sequencer.dispatchNext().dispatchedUpload).isNull()
      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        markRawImpressionUploadModelLineLabeling(any())
      }
      stubNonMemoizedFilesAndJobs()
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})

      assertThat(sequencer.dispatchNext().dispatchedUpload).isEqualTo(created.name)
      verifyBlocking(rawImpressionUploadModelLineService) {
        markRawImpressionUploadModelLineLabeling(any())
      }
      verifyBlocking(vidLabelingJobService) { batchCreateVidLabelingJobs(any()) }
      verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(any()) }
    }

  @Test
  fun `dispatchNext resumes an already claimed non-memoized line after an empty listing`() =
    runBlocking<Unit> {
      stubUploads(active = listOf(upload("upload-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
      stubModelLines(
        createdModelLine().copy { state = RawImpressionUploadModelLine.State.LABELING }
      )
      stubShardResolution(memoized = false)
      stubModelLine()
      whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
        .thenReturn(listRawImpressionUploadFilesResponse {})
      val sequencer = createSequencer()
      sequencer.dispatchNext()
      verifyBlocking(vidLabelingJobService, never()) { batchCreateVidLabelingJobs(any()) }

      stubNonMemoizedFilesAndJobs()
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      sequencer.dispatchNext()

      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        markRawImpressionUploadModelLineLabeling(any())
      }
      verifyBlocking(vidLabelingJobService) { batchCreateVidLabelingJobs(any()) }
      verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(any()) }
    }

  @Test
  fun `dispatchNext bundles all non-memoized model lines of an upload into shared jobs`() =
    runBlocking<Unit> {
      val created = upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW)
      stubUploads(created = listOf(created))
      stubModelLinesByParent(
        mapOf(
          created.name to
            listOf(
              modelLine(created.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED),
              modelLine(created.name, MODEL_LINE_2, RawImpressionUploadModelLine.State.CREATED),
            )
        )
      )
      stubShardResolution(memoized = false)
      stubModelLine()
      stubNonMemoizedFilesAndJobs()
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      stubMarkTransitions()

      createSequencer().dispatchNext()

      // One set of bin-packed jobs is shared across both model lines (NOT one set per line): each
      // created VidLabelingJob lists both model lines, with one WorkItem per job.
      val jobCaptor = argumentCaptor<BatchCreateVidLabelingJobsRequest>()
      verifyBlocking(vidLabelingJobService) { batchCreateVidLabelingJobs(jobCaptor.capture()) }
      for (createRequest in jobCaptor.firstValue.requestsList) {
        assertThat(createRequest.vidLabelingJob.cmmsModelLinesList)
          .containsExactly(MODEL_LINE, MODEL_LINE_2)
      }
      val wiCaptor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) {
        createWorkItem(wiCaptor.capture())
      }
      // Each WorkItem carries the bundled lines in model_lines (the payload); override_model_lines
      // stays empty (reserved for the operator-header override).
      for (request in wiCaptor.allValues) {
        val params =
          request.workItem.workItemParams
            .unpack<WorkItemParams>()
            .appParams
            .unpack<VidLabelerParams>()
        assertThat(params.modelLinesList).containsExactly(MODEL_LINE, MODEL_LINE_2)
        assertThat(params.overrideModelLinesList).isEmpty()
      }
      // Each bundled model line is transitioned to LABELING.
      verifyBlocking(rawImpressionUploadModelLineService, times(2)) {
        markRawImpressionUploadModelLineLabeling(any())
      }
    }

  @Test
  fun `dispatchNext threads the model line active window into each non-memoized WorkItem`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = false)
      stubModelLine()
      stubNonMemoizedFilesAndJobs()
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      stubMarkTransitions()

      createSequencer().dispatchNext()

      val captor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(captor.capture()) }
      val modelLineConfig =
        captor.firstValue.workItem.workItemParams
          .unpack<WorkItemParams>()
          .appParams
          .unpack<VidLabelerParams>()
          .modelLineConfigsMap
          .getValue(MODEL_LINE)
      assertThat(modelLineConfig.activeStartTime).isEqualTo(ACTIVE_START_TIME)
      assertThat(modelLineConfig.activeEndTime).isEqualTo(ACTIVE_END_TIME)
      // The per-impression entity-key mappings survive the per-WorkItem ModelLineConfig rebuild.
      assertThat(modelLineConfig.requiredEntityKeyFieldMappingMap)
        .containsExactly("household", "hh_col")
      assertThat(modelLineConfig.optionalEntityKeyFieldMappingMap)
        .containsExactly("creative", "cr_col")
      // The event-template descriptor + type also survive that rebuild (Phase-2 requires them).
      assertThat(modelLineConfig.eventTemplateDescriptorBlobUri)
        .isEqualTo(EVENT_TEMPLATE_DESCRIPTOR_BLOB_URI)
      assertThat(modelLineConfig.eventTemplateType).isEqualTo(EVENT_TEMPLATE_TYPE)
      // And the PopulationSpec, which Phase-2 requires to set the assigned VID's population
      // attributes. This rebuild silently drops anything not copied explicitly.
      assertThat(modelLineConfig.populationSpecBlobUri).isEqualTo(POPULATION_SPEC_BLOB_URI)
    }

  @Test
  fun `dispatchNext omits active_end_time for an open-ended non-memoized model line`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = false)
      stubNonMemoizedFilesAndJobs()
      whenever(modelLinesService.getModelLine(any()))
        .thenReturn(
          modelLine {
            name = MODEL_LINE
            activeStartTime = ACTIVE_START_TIME
          }
        )
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      stubMarkTransitions()

      createSequencer().dispatchNext()

      val captor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(captor.capture()) }
      val modelLineConfig =
        captor.firstValue.workItem.workItemParams
          .unpack<WorkItemParams>()
          .appParams
          .unpack<VidLabelerParams>()
          .modelLineConfigsMap
          .getValue(MODEL_LINE)
      assertThat(modelLineConfig.activeStartTime).isEqualTo(ACTIVE_START_TIME)
      assertThat(modelLineConfig.hasActiveEndTime()).isFalse()
    }

  @Test
  fun `dispatchNext passes the model line etag on the Mark request`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = false)
      stubModelLine()
      stubNonMemoizedFilesAndJobs()
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      stubMarkTransitions()

      createSequencer().dispatchNext()

      val captor = argumentCaptor<MarkRawImpressionUploadModelLineLabelingRequest>()
      verifyBlocking(rawImpressionUploadModelLineService) {
        markRawImpressionUploadModelLineLabeling(captor.capture())
      }
      assertThat(captor.firstValue.etag).isEqualTo(ETAG)
    }

  @Test
  fun `dispatchNext skips a model line whose claim was lost with ABORTED`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = false)
      stubModelLine()
      stubNonMemoizedFilesAndJobs()
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      // Another concurrent dispatcher claimed the model line first: the etag CAS fails with
      // ABORTED.
      whenever(rawImpressionUploadModelLineService.markRawImpressionUploadModelLineLabeling(any()))
        .thenAnswer { throw StatusException(Status.ABORTED.withDescription("etag mismatch")) }

      // dispatchNext must not propagate the lost-race error.
      val result = createSequencer().dispatchNext()

      assertThat(result.dispatchedUpload).isNull()
      verifyBlocking(vidLabelingJobService, never()) { batchCreateVidLabelingJobs(any()) }
      verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
    }

  @Test
  fun `dispatchNext publishes no work when a correction fence rejects the claim`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = false)
      stubModelLine()
      stubNonMemoizedFilesAndJobs()
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      whenever(rawImpressionUploadModelLineService.markRawImpressionUploadModelLineLabeling(any()))
        .thenAnswer {
          throw StatusException(Status.FAILED_PRECONDITION.withDescription("already LABELING"))
        }

      val result = createSequencer().dispatchNext()

      assertThat(result.dispatchedUpload).isNull()
      verifyBlocking(vidLabelingJobService, never()) { batchCreateVidLabelingJobs(any()) }
      verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
    }

  @Test
  fun `dispatchNext tolerates an already-existing WorkItem`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = false)
      stubModelLine()
      stubNonMemoizedFilesAndJobs()
      // A concurrent dispatcher created the WorkItem with the same deterministic ID first.
      whenever(workItemsService.createWorkItem(any())).thenAnswer {
        throw StatusException(Status.ALREADY_EXISTS.withDescription("work item exists"))
      }
      stubMarkTransitions()

      val result = createSequencer().dispatchNext()

      // Idempotent: the upload is still dispatched and the transition still happens.
      assertThat(result.dispatchedUpload).isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1")
      verifyBlocking(rawImpressionUploadModelLineService) {
        markRawImpressionUploadModelLineLabeling(any())
      }
    }

  @Test
  fun `dispatchNext skips dispatch when the model shard cannot be resolved`() = runBlocking {
    stubUploads(created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW)))
    stubModelLines(createdModelLine())
    // No rollout found -> shard cannot be resolved.
    whenever(modelRolloutsService.listModelRollouts(any())).thenReturn(listModelRolloutsResponse {})

    val result = createSequencer().dispatchNext()

    // Unresolvable shard: nothing dispatched, model line left CREATED for a later attempt.
    assertThat(result.dispatchedUpload).isNull()
    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      markRawImpressionUploadModelLineLabeling(any())
    }
  }

  @Test
  fun `resolveShardInfo returns shard info when a shard is available`() = runBlocking {
    stubShardResolution(memoized = true)

    val shardInfo = createSequencer().resolveShardInfo(MODEL_LINE)

    assertThat(shardInfo).isNotNull()
    assertThat(shardInfo!!.modelBlobPath).isEqualTo(MODEL_BLOB_PATH)
    assertThat(shardInfo.memoizationEnabled).isTrue()
  }

  @Test
  fun `resolveShardInfo returns null when no rollout is found`() = runBlocking {
    whenever(modelRolloutsService.listModelRollouts(any())).thenReturn(listModelRolloutsResponse {})

    val shardInfo = createSequencer().resolveShardInfo(MODEL_LINE)

    assertThat(shardInfo).isNull()
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edp123"
    private const val MODEL_SUITE = "modelProviders/mp1/modelSuites/ms1"
    private const val MODEL_LINE = "$MODEL_SUITE/modelLines/ml1"
    private const val MODEL_LINE_2 = "$MODEL_SUITE/modelLines/ml2"
    private const val MODEL_RELEASE = "$MODEL_SUITE/modelReleases/mr1"
    private const val MODEL_BLOB_PATH = "gs://models/vid-model-v1.pb"
    private const val QUEUE_NAME = "queues/vid-labeler-queue"
    private const val POOL_ASSIGNER_QUEUE_NAME = "queues/pool-assigner-queue"
    private const val NUMBER_OF_SHARDS = 2
    private const val MAX_FILE_BATCH_SIZE_BYTES = 1000L
    private const val ETAG = "W/\"abc123\""
    private const val EVENT_TEMPLATE_DESCRIPTOR_BLOB_URI =
      "gs://descriptors/event-template-set.binpb"
    private const val EVENT_TEMPLATE_TYPE =
      "wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
    private const val POPULATION_SPEC_BLOB_URI = "gs://configs/population-spec.textproto"
    private val TRACE_CONTEXT =
      mapOf("traceparent" to "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")

    private val FIXED_NOW: Instant = Instant.parse("2026-06-03T12:00:00Z")
    private val ACTIVE_START_TIME: Timestamp = Timestamps.fromSeconds(1_600_000_000L)
    private val ACTIVE_END_TIME: Timestamp = Timestamps.fromSeconds(1_700_000_000L)

    private val VID_LABELER_PARAMS_TEMPLATE: VidLabelerParams = vidLabelerParams {
      dataProvider = DATA_PROVIDER
      rawImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "gs://raw-impressions-bucket"
        }
      vidLabeledImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "gs://vid-labeled-bucket"
        }
    }

    private val SUBPOOL_ASSIGNER_PARAMS_TEMPLATE: SubpoolAssignerParams = subpoolAssignerParams {
      dataProvider = DATA_PROVIDER
      rawImpressionStorageParams =
        SubpoolAssignerParamsKt.storageParams {
          gcsProjectId = "test-project"
          blobPrefix = "gs://raw-impressions-bucket"
        }
      vidLabeledImpressionsStorageParams =
        SubpoolAssignerParamsKt.storageParams {
          gcsProjectId = "test-project"
          blobPrefix = "gs://vid-labeled-bucket"
        }
      vidRankMapStorageParams =
        SubpoolAssignerParamsKt.storageParams {
          gcsProjectId = "test-project"
          blobPrefix = "gs://vid-rank-map-bucket"
        }
      subpoolMapStorageParams =
        SubpoolAssignerParamsKt.storageParams {
          gcsProjectId = "test-project"
          blobPrefix = "gs://subpool-map-bucket"
        }
      modelStorageParams =
        SubpoolAssignerParamsKt.storageParams {
          gcsProjectId = "test-project"
          blobPrefix = "gs://model-bucket"
        }
      rawImpressionMetadataStorageConnection = transportLayerSecurityParams {
        clientCertResourcePath = "cert"
        clientPrivateKeyResourcePath = "key"
      }
      maxFileBatchSizeBytes = MAX_FILE_BATCH_SIZE_BYTES
    }

    private val MODEL_LINE_CONFIGS: Map<String, VidLabelerParams.ModelLineConfig> =
      mapOf(
        MODEL_LINE to
          VidLabelerParamsKt.modelLineConfig {
            labelerInputFieldMapping +=
              LabelerInputFieldMapping.newBuilder()
                .setFieldPath("age")
                .setScalar(ScalarColumn.newBuilder().setColumn("user_age"))
                .build()
            labelerInputFieldMapping +=
              LabelerInputFieldMapping.newBuilder()
                .setFieldPath("gender")
                .setScalar(ScalarColumn.newBuilder().setColumn("user_gender"))
                .build()
            requiredEntityKeyFieldMapping["household"] = "hh_col"
            optionalEntityKeyFieldMapping["creative"] = "cr_col"
            eventTemplateDescriptorBlobUri = EVENT_TEMPLATE_DESCRIPTOR_BLOB_URI
            eventTemplateType = EVENT_TEMPLATE_TYPE
            populationSpecBlobUri = POPULATION_SPEC_BLOB_URI
          },
        MODEL_LINE_2 to
          VidLabelerParamsKt.modelLineConfig {
            labelerInputFieldMapping +=
              LabelerInputFieldMapping.newBuilder()
                .setFieldPath("age")
                .setScalar(ScalarColumn.newBuilder().setColumn("user_age"))
                .build()
          },
      )
  }
}
