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
import com.google.protobuf.ByteString
import com.google.protobuf.util.Timestamps
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.metrics.export.MetricReader
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.atLeastOnce
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
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelShard
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ScalarColumn
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.transportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GetWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

/**
 * Tests for [VidLabelingMonitor].
 *
 * The dispatch mechanics (oldest-upload selection, model-shard resolution, WorkItem creation, etag
 * CAS) are owned by [VidLabelingDispatchSequencer] and covered by
 * `VidLabelingDispatchSequencerTest`. These tests verify the monitor's own responsibilities:
 * delegating to the sequencer and surfacing its result, and the failure/staleness checks.
 */
@RunWith(JUnit4::class)
class VidLabelingMonitorTest {

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
  private val modelRolloutsService: ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase =
    mockService()
  private val modelShardsService: ModelShardsGrpcKt.ModelShardsCoroutineImplBase = mockService()
  private val modelLinesService: ModelLinesGrpcKt.ModelLinesCoroutineImplBase = mockService()
  private val rawImpressionUploadFileService:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase =
    mockService()
  private val vidLabelingJobService:
    VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase =
    mockService()
  private val rankerJobService: RankerJobServiceGrpcKt.RankerJobServiceCoroutineImplBase =
    mockService()

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
    addService(rankerJobService)
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
  private val rankerJobStub by lazy {
    RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub(grpcTestServerRule.channel)
  }
  private val rawImpressionsStorageClient = InMemoryStorageClient()
  private val vidLabeledImpressionsStorageClient = InMemoryStorageClient()

  private val fixedClock: Clock = Clock.fixed(FIXED_NOW, ZoneId.of("UTC"))

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

  /** Single long-gauge point value for [name], failing if the metric was never emitted. */
  private fun List<MetricData>.gaugeValue(name: String): Long =
    associateBy { it.name }.getValue(name).longGaugeData.points.single().value

  /** Single long-counter point value for [name], failing if the metric was never emitted. */
  private fun List<MetricData>.counterValue(name: String): Long =
    associateBy { it.name }.getValue(name).longSumData.points.single().value

  private fun createSequencer(): VidLabelingDispatchSequencer =
    VidLabelingDispatchSequencer(
      rawImpressionUploadStub = rawImpressionUploadStub,
      rawImpressionUploadModelLineStub = rawImpressionUploadModelLineStub,
      workItemsStub = workItemsStub,
      poolAssignmentJobStub = poolAssignmentJobStub,
      modelRolloutsStub = modelRolloutsStub,
      modelShardsStub = modelShardsStub,
      modelLinesStub = modelLinesStub,
      dataProviderName = DATA_PROVIDER,
      vidLabelerParamsTemplate = VID_LABELER_PARAMS_TEMPLATE,
      subpoolAssignerParamsTemplate = SubpoolAssignerParams.getDefaultInstance(),
      queueName = QUEUE_NAME,
      poolAssignerQueueName = POOL_ASSIGNER_QUEUE_NAME,
      numberOfShards = NUMBER_OF_SHARDS,
      modelLineConfigs = MODEL_LINE_CONFIGS,
      rawImpressionUploadFileStub = rawImpressionUploadFileStub,
      vidLabelingJobStub = vidLabelingJobStub,
      maxFileBatchSizeBytes = MAX_FILE_BATCH_SIZE_BYTES,
    )

  private fun createMonitor(): VidLabelingMonitor =
    VidLabelingMonitor(
      rawImpressionUploadStub = rawImpressionUploadStub,
      rawImpressionUploadModelLineStub = rawImpressionUploadModelLineStub,
      dispatchSequencer = createSequencer(),
      dataProviderName = DATA_PROVIDER,
      stalenessThreshold = STALENESS_THRESHOLD,
      rawImpressionUploadFileStub = rawImpressionUploadFileStub,
      rawImpressionsStorageClientProvider = { rawImpressionsStorageClient },
      vidLabeledImpressionsStorageClientProvider = { vidLabeledImpressionsStorageClient },
      rankerJobStub = rankerJobStub,
      vidLabelingJobStub = vidLabelingJobStub,
      workItemsStub = workItemsStub,
      clock = fixedClock,
    )

  /**
   * Stubs `listRawImpressionUploads` to return [active] for the `ACTIVE` filter, [created] for the
   * `CREATED` filter, and [failed] for the `FAILED` filter.
   */
  private suspend fun stubUploads(
    active: List<RawImpressionUpload> = emptyList(),
    created: List<RawImpressionUpload> = emptyList(),
    failed: List<RawImpressionUpload> = emptyList(),
    completed: List<RawImpressionUpload> = emptyList(),
  ) {
    whenever(rawImpressionUploadService.listRawImpressionUploads(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<ListRawImpressionUploadsRequest>(0)
      val states = request.filter.stateInList
      // An empty state_in matches all states (the monitor's single-snapshot list); a specific
      // state_in (the dispatcher's dispatch scan) returns just those states.
      val uploads =
        if (states.isEmpty()) {
          active + created + failed + completed
        } else {
          states.flatMap { state ->
            when (state) {
              RawImpressionUpload.State.ACTIVE -> active
              RawImpressionUpload.State.CREATED -> created
              RawImpressionUpload.State.FAILED -> failed
              RawImpressionUpload.State.COMPLETED -> completed
              else -> emptyList()
            }
          }
        }
      listRawImpressionUploadsResponse { rawImpressionUploads += uploads }
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

  /** Stubs `listRawImpressionUploadModelLines` to return distinct model lines per parent upload. */
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

  private fun modelLine(
    parentUpload: String,
    cmmsLine: String,
    modelLineState: RawImpressionUploadModelLine.State,
  ) = rawImpressionUploadModelLine {
    name = "$parentUpload/modelLines/ml1"
    cmmsModelLine = cmmsLine
    state = modelLineState
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
    whenever(
        rawImpressionUploadModelLineService.markRawImpressionUploadModelLinePoolAssigning(any())
      )
      .thenReturn(rawImpressionUploadModelLine {})
  }

  /**
   * Stubs the non-memoized fan-out RPCs: `listRawImpressionUploadFiles` returns [NUMBER_OF_SHARDS]
   * files each sized at the batch threshold (one batch per file), and `batchCreateVidLabelingJobs`
   * echoes one job per request named `${parent}/vidLabelingJobs/job-$i`.
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

  private fun upload(id: String, state: RawImpressionUpload.State, createdAt: Instant) =
    rawImpressionUpload {
      name = "$DATA_PROVIDER/rawImpressionUploads/$id"
      this.state = state
      createTime = Timestamps.fromMillis(createdAt.toEpochMilli())
      doneBlobUri = "gs://raw-bucket/edp7/2026-06-01/done"
    }

  private fun createdModelLine(id: String = "ml1") = rawImpressionUploadModelLine {
    name = "$DATA_PROVIDER/rawImpressionUploads/upload-1/modelLines/$id"
    cmmsModelLine = MODEL_LINE
    state = RawImpressionUploadModelLine.State.CREATED
  }

  @Test
  fun `run delegates dispatch to the sequencer and surfaces the dispatched upload`() = runBlocking {
    stubUploads(created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW)))
    stubModelLines(createdModelLine())
    stubShardResolution(memoized = false)
    stubNonMemoizedFilesAndJobs()
    whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
    stubMarkTransitions()

    val result = createMonitor().runDispatch()

    assertThat(result.dispatchedUpload).isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1")
    assertThat(result.dispatchError).isFalse()

    val metrics = collectMetrics()
    assertThat(metrics.counterValue(UPLOADS_DISPATCHED_METRIC)).isEqualTo(1)
    assertThat(metrics.gaugeValue(UPLOADS_QUEUED_METRIC)).isEqualTo(0)
  }

  @Test
  fun `run reports queued model lines and dispatches none when the line is already in progress`() =
    runBlocking {
      val active = upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)
      val queued = upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW)
      stubUploads(active = listOf(active), created = listOf(queued))
      // The active upload's model line is already running for MODEL_LINE, so the created upload's
      // model line for the same MODEL_LINE is held back rather than dispatched.
      stubModelLinesByParent(
        mapOf(
          active.name to
            listOf(modelLine(active.name, MODEL_LINE, RawImpressionUploadModelLine.State.LABELING)),
          queued.name to
            listOf(modelLine(queued.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED)),
        )
      )

      val result = createMonitor().runDispatch()

      assertThat(result.dispatchedUpload).isNull()
      assertThat(result.queuedUploads).isEqualTo(1)
      verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
      assertThat(collectMetrics().gaugeValue(UPLOADS_QUEUED_METRIC)).isEqualTo(1)
    }

  @Test
  fun `run reports a dispatch error and sets the gauge when the sequencer throws`() = runBlocking {
    // The sequencer's first RPC (listUploads) fails, so dispatchNext() throws.
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenThrow(StatusRuntimeException(Status.UNAVAILABLE))

    val result = createMonitor().runDispatch()

    assertThat(result.dispatchError).isTrue()
    assertThat(result.dispatchedUpload).isNull()

    val metricByName = collectMetrics().associateBy { it.name }
    assertThat(metricByName).containsKey(DISPATCH_ERRORS_METRIC)
    val point = metricByName.getValue(DISPATCH_ERRORS_METRIC).longGaugeData.points.single()
    assertThat(point.value).isEqualTo(1)
    assertThat(point.attributes.get(VidLabelingMonitorMetrics.DATA_PROVIDER_ATTR))
      .isEqualTo(DATA_PROVIDER)
  }

  @Test
  fun `run reports uploads stuck past the staleness SLA`() = runBlocking {
    val stuck =
      upload(
        "active-stuck",
        RawImpressionUpload.State.ACTIVE,
        FIXED_NOW.minus(STALENESS_THRESHOLD).minusSeconds(60),
      )
    stubUploads(active = listOf(stuck))
    stubModelLines() // No failed model lines on the stuck upload.

    val result = createMonitor().runHealth()

    assertThat(result.stuckUploads)
      .containsExactly("$DATA_PROVIDER/rawImpressionUploads/active-stuck")
    assertThat(result.hasIssues).isTrue()
    assertThat(collectMetrics().gaugeValue(UPLOADS_STUCK_METRIC)).isEqualTo(1)
  }

  @Test
  fun `run does not flag a recent ACTIVE upload as stuck`() = runBlocking {
    stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
    stubModelLines()

    val result = createMonitor().runHealth()

    assertThat(result.stuckUploads).isEmpty()
    assertThat(result.hasIssues).isFalse()
  }

  @Test
  fun `run reports FAILED model lines on ACTIVE uploads`() = runBlocking {
    stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
    val failedModelLine = rawImpressionUploadModelLine {
      name = "$DATA_PROVIDER/rawImpressionUploads/active-1/modelLines/ml1"
      cmmsModelLine = MODEL_LINE
      state = RawImpressionUploadModelLine.State.FAILED
    }
    stubModelLines(failedModelLine)

    val result = createMonitor().runHealth()

    assertThat(result.failedModelLines)
      .containsExactly("$DATA_PROVIDER/rawImpressionUploads/active-1/modelLines/ml1")
    assertThat(result.hasIssues).isTrue()
    assertThat(collectMetrics().gaugeValue(FAILED_UPLOADS_METRIC)).isEqualTo(1)
  }

  @Test
  fun `run counts one failed upload when a single upload has multiple FAILED model lines`() =
    runBlocking {
      stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
      stubModelLines(
        rawImpressionUploadModelLine {
          name = "$DATA_PROVIDER/rawImpressionUploads/active-1/modelLines/ml1"
          cmmsModelLine = MODEL_LINE
          state = RawImpressionUploadModelLine.State.FAILED
        },
        rawImpressionUploadModelLine {
          name = "$DATA_PROVIDER/rawImpressionUploads/active-1/modelLines/ml2"
          cmmsModelLine = MODEL_LINE
          state = RawImpressionUploadModelLine.State.FAILED
        },
      )

      val result = createMonitor().runHealth()

      // Both FAILED lines surface, but failed_uploads counts the single upload, not 2.
      assertThat(result.failedModelLines).hasSize(2)
      assertThat(collectMetrics().gaugeValue(FAILED_UPLOADS_METRIC)).isEqualTo(1)
    }

  @Test
  fun `run reports a CREATED upload stuck past the staleness SLA`() = runBlocking {
    // An ACTIVE upload already holds MODEL_LINE, so the sequencer cannot activate the CREATED
    // upload; it then ages past the SLA and must be surfaced as stuck.
    val active = upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)
    val stuckCreated =
      upload(
        "created-stuck",
        RawImpressionUpload.State.CREATED,
        FIXED_NOW.minus(STALENESS_THRESHOLD).minusSeconds(60),
      )
    stubUploads(active = listOf(active), created = listOf(stuckCreated))
    stubModelLinesByParent(
      mapOf(
        active.name to
          listOf(modelLine(active.name, MODEL_LINE, RawImpressionUploadModelLine.State.LABELING)),
        stuckCreated.name to
          listOf(
            modelLine(stuckCreated.name, MODEL_LINE, RawImpressionUploadModelLine.State.CREATED)
          ),
      )
    )

    val result = createMonitor().runHealth()

    assertThat(result.stuckUploads)
      .containsExactly("$DATA_PROVIDER/rawImpressionUploads/created-stuck")
    assertThat(result.hasIssues).isTrue()
  }

  @Test
  fun `run reports FAILED model lines on a FAILED upload`() = runBlocking {
    // A FAILED upload (state rolled up after its model lines terminated) is terminal, so it is not
    // flagged as stuck, but its FAILED model lines must still surface.
    stubUploads(failed = listOf(upload("failed-1", RawImpressionUpload.State.FAILED, FIXED_NOW)))
    stubModelLines(
      rawImpressionUploadModelLine {
        name = "$DATA_PROVIDER/rawImpressionUploads/failed-1/modelLines/ml1"
        cmmsModelLine = MODEL_LINE
        state = RawImpressionUploadModelLine.State.FAILED
      }
    )

    val result = createMonitor().runHealth()

    assertThat(result.stuckUploads).isEmpty()
    assertThat(result.failedModelLines)
      .containsExactly("$DATA_PROVIDER/rawImpressionUploads/failed-1/modelLines/ml1")
    assertThat(result.hasIssues).isTrue()
  }

  private suspend fun seedRaw(vararg keys: String) {
    for (key in keys) {
      rawImpressionsStorageClient.writeBlob(key, flowOf(ByteString.copyFromUtf8("x")))
    }
  }

  private suspend fun seedLabeled(vararg keys: String) {
    for (key in keys) {
      vidLabeledImpressionsStorageClient.writeBlob(key, flowOf(ByteString.copyFromUtf8("x")))
    }
  }

  private suspend fun stubFiles(vararg files: RawImpressionUploadFile) {
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
      .thenReturn(
        listRawImpressionUploadFilesResponse { rawImpressionUploadFiles += files.toList() }
      )
  }

  /**
   * Stubs `listRankerJobs` for a `(upload, model line)`: the unfiltered count returns [total], the
   * CREATED/FAILED count returns [nonSucceeded], and the SUCCEEDED page returns one job named
   * [succeededJobName].
   */
  private suspend fun stubRankerJobs(total: Int, nonSucceeded: Int, succeededJobName: String) {
    whenever(rankerJobService.listRankerJobs(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<ListRankerJobsRequest>(0)
      val states = request.filter.stateInList
      listRankerJobsResponse {
        when {
          states.isEmpty() -> totalSize = total
          states.contains(RankerJob.State.SUCCEEDED) -> rankerJobs += rankerJob {
              name = succeededJobName
            }
          else -> totalSize = nonSucceeded
        }
      }
    }
  }

  @Test
  fun `reports missing done blobs for a registered date folder without a done blob`() =
    runBlocking {
      stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
      stubModelLines()
      stubFiles(
        rawImpressionUploadFile {
          blobUri = "gs://raw-bucket/edp7/2026-06-02/data-1"
          eventDate = date {
            year = 2026
            month = 6
            day = 2
          }
        }
      )
      seedRaw("edp7/2026-06-02/data-1")

      createMonitor().runHealth()

      assertThat(collectMetrics().gaugeValue("edpa.vid_labeling_monitor.missing_done_blobs"))
        .isEqualTo(1)
    }

  @Test
  fun `reports zero-impression dates for a done blob with no data files`() = runBlocking {
    stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
    stubModelLines()
    stubFiles(
      rawImpressionUploadFile {
        blobUri = "gs://raw-bucket/edp7/2026-06-01/data-1"
        eventDate = date {
          year = 2026
          month = 6
          day = 1
        }
      }
    )
    seedRaw("edp7/2026-06-01/done")

    createMonitor().runHealth()

    val metrics = collectMetrics()
    assertThat(metrics.gaugeValue("edpa.vid_labeling_monitor.zero_impression_dates")).isEqualTo(1)
    assertThat(metrics.gaugeValue("edpa.vid_labeling_monitor.missing_done_blobs")).isEqualTo(0)
  }

  @Test
  fun `reports late-arriving files uploaded after the done blob`() = runBlocking {
    stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
    stubModelLines()
    stubFiles(
      rawImpressionUploadFile {
        blobUri = "gs://raw-bucket/edp7/2026-06-01/data-late"
        eventDate = date {
          year = 2026
          month = 6
          day = 1
        }
      }
    )
    seedRaw("edp7/2026-06-01/done")
    kotlinx.coroutines.delay(5)
    seedRaw("edp7/2026-06-01/data-late")

    createMonitor().runHealth()

    assertThat(collectMetrics().gaugeValue("edpa.vid_labeling_monitor.late_arriving_files"))
      .isEqualTo(1)
  }

  @Test
  fun `data-quality check failed gauge is 0 on a healthy run`() = runBlocking {
    stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
    stubModelLines()
    stubFiles()

    createMonitor().runHealth()

    assertThat(collectMetrics().gaugeValue("edpa.vid_labeling_monitor.data_quality_check_failed"))
      .isEqualTo(0)
  }

  @Test
  fun `flags data-quality check failure and stays non-blocking when the crawl throws`() =
    runBlocking {
      stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
      stubModelLines()
      whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
        .thenThrow(StatusRuntimeException(Status.INTERNAL))

      // Non-blocking: the health run completes despite the crawl failure.
      createMonitor().runHealth()

      assertThat(collectMetrics().gaugeValue("edpa.vid_labeling_monitor.data_quality_check_failed"))
        .isEqualTo(1)
    }

  @Test
  fun `reports missing raw files for a registered file absent from storage`() = runBlocking {
    // Directional metadata -> storage signal: the file is registered (with an event_date) but its
    // blob is gone from storage (data loss).
    stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
    stubModelLines()
    stubFiles(
      rawImpressionUploadFile {
        blobUri = "gs://raw-bucket/edp7/2026-06-01/data-1"
        eventDate = date {
          year = 2026
          month = 6
          day = 1
        }
      }
    )

    createMonitor().runHealth()

    assertThat(collectMetrics().gaugeValue("edpa.vid_labeling_monitor.missing_raw_files"))
      .isEqualTo(1)
  }

  @Test
  fun `reports missing labeled outputs for a completed model line and date with no done blob`() =
    runBlocking {
      stubUploads(
        completed = listOf(upload("done-1", RawImpressionUpload.State.COMPLETED, FIXED_NOW))
      )
      stubModelLines(
        modelLine(
          "$DATA_PROVIDER/rawImpressionUploads/done-1",
          MODEL_LINE,
          RawImpressionUploadModelLine.State.COMPLETED,
        )
      )
      // One registered file dated 2026-06-01; no labeled done blob for (ml1, 2026-06-01).
      stubFiles(
        rawImpressionUploadFile {
          blobUri = "gs://raw-bucket/edp7/2026-06-01/data-1"
          eventDate = date {
            year = 2026
            month = 6
            day = 1
          }
        }
      )

      createMonitor().runHealth()

      assertThat(collectMetrics().gaugeValue("edpa.vid_labeling_monitor.missing_labeled_outputs"))
        .isEqualTo(1)
    }

  @Test
  fun `does not report missing labeled outputs when the labeled done blob exists`() = runBlocking {
    // The labeler writes model-line/<id>/<date>/done for the input event date whether or not any
    // impression survived filtering, so a fully-dropped date is finalized and not a false positive.
    stubUploads(
      completed = listOf(upload("done-1", RawImpressionUpload.State.COMPLETED, FIXED_NOW))
    )
    stubModelLines(
      modelLine(
        "$DATA_PROVIDER/rawImpressionUploads/done-1",
        MODEL_LINE,
        RawImpressionUploadModelLine.State.COMPLETED,
      )
    )
    stubFiles(
      rawImpressionUploadFile {
        blobUri = "gs://raw-bucket/edp7/2026-06-01/data-1"
        eventDate = date {
          year = 2026
          month = 6
          day = 1
        }
      }
    )
    seedLabeled("model-line/ml1/2026-06-01/done")

    createMonitor().runHealth()

    assertThat(collectMetrics().gaugeValue("edpa.vid_labeling_monitor.missing_labeled_outputs"))
      .isEqualTo(0)
  }

  @Test
  fun `recovers a stuck RANKING model line by re-publishing a WorkItem`() = runBlocking {
    stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
    stubModelLines(
      rawImpressionUploadModelLine {
        name = "$DATA_PROVIDER/rawImpressionUploads/active-1/modelLines/ml1"
        cmmsModelLine = MODEL_LINE
        state = RawImpressionUploadModelLine.State.RANKING
      }
    )
    stubRankerJobs(
      total = 2,
      nonSucceeded = 0,
      succeededJobName = "$DATA_PROVIDER/rawImpressionUploads/active-1/rankerJobs/rj1",
    )
    // The monitor must re-derive the EXACT WorkItem id the SubpoolAssigner published for the ranker
    // job (WorkItemIds.forVidRankBuilder); only that name resolves, any other one 404s.
    whenever(workItemsService.getWorkItem(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<GetWorkItemRequest>(0)
      if (request.name == "workItems/$RANKER_WORK_ITEM") {
        workItem { queue = "queues/ranker" }
      } else {
        throw Status.NOT_FOUND.asRuntimeException()
      }
    }
    whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})

    val result = createMonitor().runHealth()

    assertThat(result.recoveredTransitions).isEqualTo(1)
    assertThat(
        collectMetrics().counterValue("edpa.vid_labeling_monitor.phase_transitions_recovered")
      )
      .isEqualTo(1)
    val createCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService) { createWorkItem(createCaptor.capture()) }
    assertThat(createCaptor.firstValue.workItemId)
      .isEqualTo("$RANKER_WORK_ITEM-monitor-recovery-1")
    assertThat(createCaptor.firstValue.workItem.queue).isEqualTo("queues/ranker")
    // C3: a successful recovery is not an issue; only exhausted recovery is.
    assertThat(result.hasIssues).isFalse()
  }

  @Test
  fun `recovers a stuck LABELING model line by re-publishing a WorkItem`() = runBlocking {
    stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
    stubModelLines(
      rawImpressionUploadModelLine {
        name = "$DATA_PROVIDER/rawImpressionUploads/active-1/modelLines/ml1"
        cmmsModelLine = MODEL_LINE
        state = RawImpressionUploadModelLine.State.LABELING
      }
    )
    whenever(vidLabelingJobService.listVidLabelingJobs(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<ListVidLabelingJobsRequest>(0)
      listVidLabelingJobsResponse {
        if (request.filter.state == VidLabelingJob.State.SUCCEEDED) {
          vidLabelingJobs += vidLabelingJob {
            name = "$DATA_PROVIDER/rawImpressionUploads/active-1/vidLabelingJobs/vj1"
          }
        }
      }
    }
    // A memoized model line reaches LABELING via the ranker fan-out, whose Phase-2 WorkItem the
    // ranker names via WorkItemIds.forVidLabeler (a bounded "vl-" + hash). The monitor MUST
    // re-derive that same id through WorkItemIds; any other form 404s and recovery silently no-ops.
    whenever(workItemsService.getWorkItem(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<GetWorkItemRequest>(0)
      if (request.name == "workItems/$VID_LABELER_WORK_ITEM") {
        workItem { queue = "queues/labeler" }
      } else {
        throw Status.NOT_FOUND.asRuntimeException()
      }
    }
    whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})

    val result = createMonitor().runHealth()

    assertThat(result.recoveredTransitions).isEqualTo(1)
    val createCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService) { createWorkItem(createCaptor.capture()) }
    assertThat(createCaptor.firstValue.workItemId).isEqualTo("$VID_LABELER_WORK_ITEM-monitor-recovery-1")
    assertThat(createCaptor.firstValue.workItem.queue).isEqualTo("queues/labeler")
  }

  @Test
  fun `does not recover a model line that is not yet stuck`() = runBlocking {
    stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
    stubModelLines(
      rawImpressionUploadModelLine {
        name = "$DATA_PROVIDER/rawImpressionUploads/active-1/modelLines/ml1"
        cmmsModelLine = MODEL_LINE
        state = RawImpressionUploadModelLine.State.RANKING
        updateTime = Timestamps.fromMillis(FIXED_NOW.toEpochMilli())
      }
    )

    val result = createMonitor().runHealth()

    assertThat(result.recoveredTransitions).isEqualTo(0)
    assertThat(
        collectMetrics().none { it.name == "edpa.vid_labeling_monitor.phase_transitions_recovered" }
      )
      .isTrue()
  }

  @Test
  fun `recovery publishes the next suffix when an earlier attempt already exists`() = runBlocking {
    stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
    stubModelLines(
      rawImpressionUploadModelLine {
        name = "$DATA_PROVIDER/rawImpressionUploads/active-1/modelLines/ml1"
        cmmsModelLine = MODEL_LINE
        state = RawImpressionUploadModelLine.State.RANKING
      }
    )
    stubRankerJobs(
      total = 2,
      nonSucceeded = 0,
      succeededJobName = "$DATA_PROVIDER/rawImpressionUploads/active-1/rankerJobs/rj1",
    )
    whenever(workItemsService.getWorkItem(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<GetWorkItemRequest>(0)
      if (request.name == "workItems/$RANKER_WORK_ITEM") workItem { queue = "queues/ranker" }
      else throw Status.NOT_FOUND.asRuntimeException()
    }
    // Attempt 1 was published on a prior tick and persists in the queue; attempt 2 is free.
    whenever(workItemsService.createWorkItem(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<CreateWorkItemRequest>(0)
      if (request.workItemId == "$RANKER_WORK_ITEM-monitor-recovery-1")
        throw Status.ALREADY_EXISTS.asRuntimeException()
      else workItem {}
    }

    val result = createMonitor().runHealth()

    assertThat(result.recoveredTransitions).isEqualTo(1)
    assertThat(result.recoveryExhausted).isEqualTo(0)
    val createCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(2)) { createWorkItem(createCaptor.capture()) }
    assertThat(createCaptor.allValues.map { it.workItemId })
      .containsExactly(
        "$RANKER_WORK_ITEM-monitor-recovery-1",
        "$RANKER_WORK_ITEM-monitor-recovery-2",
      )
      .inOrder()
  }

  @Test
  fun `recovery escalates when all attempts are exhausted`() = runBlocking {
    stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
    stubModelLines(
      rawImpressionUploadModelLine {
        name = "$DATA_PROVIDER/rawImpressionUploads/active-1/modelLines/ml1"
        cmmsModelLine = MODEL_LINE
        state = RawImpressionUploadModelLine.State.RANKING
      }
    )
    stubRankerJobs(
      total = 2,
      nonSucceeded = 0,
      succeededJobName = "$DATA_PROVIDER/rawImpressionUploads/active-1/rankerJobs/rj1",
    )
    whenever(workItemsService.getWorkItem(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<GetWorkItemRequest>(0)
      if (request.name == "workItems/$RANKER_WORK_ITEM") workItem { queue = "queues/ranker" }
      else throw Status.NOT_FOUND.asRuntimeException()
    }
    // All MAX_RECOVERY_ATTEMPTS recovery WorkItems were published on prior ticks and remain queued.
    whenever(workItemsService.createWorkItem(any()))
      .thenThrow(Status.ALREADY_EXISTS.asRuntimeException())

    val result = createMonitor().runHealth()

    assertThat(result.recoveredTransitions).isEqualTo(0)
    assertThat(result.recoveryExhausted).isEqualTo(1)
    assertThat(result.hasIssues).isTrue()
    assertThat(collectMetrics().gaugeValue("edpa.vid_labeling_monitor.recovery_exhausted"))
      .isEqualTo(1)
    verifyBlocking(workItemsService, times(3)) { createWorkItem(any()) }
  }

  @Test
  fun `recovery does not burn an attempt on a transient publish error`() = runBlocking {
    stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
    stubModelLines(
      rawImpressionUploadModelLine {
        name = "$DATA_PROVIDER/rawImpressionUploads/active-1/modelLines/ml1"
        cmmsModelLine = MODEL_LINE
        state = RawImpressionUploadModelLine.State.RANKING
      }
    )
    stubRankerJobs(
      total = 2,
      nonSucceeded = 0,
      succeededJobName = "$DATA_PROVIDER/rawImpressionUploads/active-1/rankerJobs/rj1",
    )
    whenever(workItemsService.getWorkItem(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<GetWorkItemRequest>(0)
      if (request.name == "workItems/$RANKER_WORK_ITEM") workItem { queue = "queues/ranker" }
      else throw Status.NOT_FOUND.asRuntimeException()
    }
    whenever(workItemsService.createWorkItem(any()))
      .thenThrow(Status.UNAVAILABLE.asRuntimeException())

    val result = createMonitor().runHealth()

    assertThat(result.recoveredTransitions).isEqualTo(0)
    assertThat(result.recoveryExhausted).isEqualTo(0)
    assertThat(collectMetrics().counterValue("edpa.vid_labeling_monitor.recovery_step_failures"))
      .isEqualTo(1)
    // Stops at the first attempt's transient error; the suffix is retried next tick.
    verifyBlocking(workItemsService, times(1)) { createWorkItem(any()) }
  }

  @Test
  fun `run reads uploads and model lines as one snapshot shared across checks`() = runBlocking {
    // An ACTIVE upload is read by both checkFailuresAndStaleness and recoverStuckPhases. The
    // snapshot must list uploads once (a single unfiltered request) and each upload's model lines
    // once (memoized), not once per check.
    stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
    stubModelLines(
      rawImpressionUploadModelLine {
        name = "$DATA_PROVIDER/rawImpressionUploads/active-1/modelLines/ml1"
        cmmsModelLine = MODEL_LINE
        state = RawImpressionUploadModelLine.State.LABELING
        updateTime = Timestamps.fromMillis(FIXED_NOW.toEpochMilli())
      }
    )

    createMonitor().runHealth()

    // Exactly one unfiltered ListRawImpressionUploads: the monitor snapshot. runHealth does not
    // dispatch, so there is no separate state-filtered dispatch scan this tick.
    val uploadRequests = argumentCaptor<ListRawImpressionUploadsRequest>()
    verifyBlocking(rawImpressionUploadService, atLeastOnce()) {
      listRawImpressionUploads(uploadRequests.capture())
    }
    assertThat(uploadRequests.allValues.count { it.filter.stateInList.isEmpty() }).isEqualTo(1)
    // active-1's model lines are listed exactly once this tick: the monitor snapshot, reused by
    // both
    // checkFailuresAndStaleness and recoverStuckPhases. Before the snapshot the monitor listed them
    // once per check, so this was 2. (runHealth does not dispatch, so there is no dispatcher scan.)
    verifyBlocking(rawImpressionUploadModelLineService, times(1)) {
      listRawImpressionUploadModelLines(any())
    }
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edp123"
    private const val RANKER_JOB_NAME =
      "$DATA_PROVIDER/rawImpressionUploads/active-1/rankerJobs/rj1"
    private const val VID_LABELING_JOB_NAME =
      "$DATA_PROVIDER/rawImpressionUploads/active-1/vidLabelingJobs/vj1"
    private val RANKER_WORK_ITEM = WorkItemIds.forVidRankBuilder(RANKER_JOB_NAME)
    private val VID_LABELER_WORK_ITEM = WorkItemIds.forVidLabeler(VID_LABELING_JOB_NAME)
    private const val DISPATCH_ERRORS_METRIC = "edpa.vid_labeling_monitor.dispatch_errors"
    private const val UPLOADS_QUEUED_METRIC = "edpa.vid_labeling_monitor.uploads_queued"
    private const val UPLOADS_STUCK_METRIC = "edpa.vid_labeling_monitor.uploads_stuck"
    private const val FAILED_UPLOADS_METRIC = "edpa.vid_labeling_monitor.failed_uploads"
    private const val UPLOADS_DISPATCHED_METRIC = "edpa.vid_labeling_monitor.uploads_dispatched"
    private const val MODEL_SUITE = "modelProviders/mp1/modelSuites/ms1"
    private const val MODEL_LINE = "$MODEL_SUITE/modelLines/ml1"
    private const val MODEL_RELEASE = "$MODEL_SUITE/modelReleases/mr1"
    private const val MODEL_BLOB_PATH = "gs://models/vid-model-v1.pb"
    private const val QUEUE_NAME = "queues/vid-labeler-queue"
    private const val POOL_ASSIGNER_QUEUE_NAME = "queues/pool-assigner-queue"
    private const val NUMBER_OF_SHARDS = 2
    private const val MAX_FILE_BATCH_SIZE_BYTES = 1000L

    private val FIXED_NOW: Instant = Instant.parse("2026-06-03T12:00:00Z")
    private val STALENESS_THRESHOLD: Duration = Duration.ofHours(24)

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
      vidRepoConnection = transportLayerSecurityParams {
        clientCertResourcePath = "cert"
        clientPrivateKeyResourcePath = "key"
      }
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
          }
      )
  }
}
