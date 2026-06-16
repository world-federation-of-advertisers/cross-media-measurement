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
import com.google.protobuf.util.Timestamps
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
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.never
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
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
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.transportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

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
  private val modelRolloutsService: ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase =
    mockService()
  private val modelShardsService: ModelShardsGrpcKt.ModelShardsCoroutineImplBase = mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(rawImpressionUploadService)
    addService(rawImpressionUploadModelLineService)
    addService(workItemsService)
    addService(modelRolloutsService)
    addService(modelShardsService)
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
  private val modelRolloutsStub by lazy {
    ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub(grpcTestServerRule.channel)
  }
  private val modelShardsStub by lazy {
    ModelShardsGrpcKt.ModelShardsCoroutineStub(grpcTestServerRule.channel)
  }

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

  private fun createSequencer(): VidLabelingDispatchSequencer =
    VidLabelingDispatchSequencer(
      rawImpressionUploadStub = rawImpressionUploadStub,
      rawImpressionUploadModelLineStub = rawImpressionUploadModelLineStub,
      workItemsStub = workItemsStub,
      modelRolloutsStub = modelRolloutsStub,
      modelShardsStub = modelShardsStub,
      dataProviderName = DATA_PROVIDER,
      vidLabelerParamsTemplate = VID_LABELER_PARAMS_TEMPLATE,
      queueName = QUEUE_NAME,
      numberOfShards = NUMBER_OF_SHARDS,
      modelLineConfigs = MODEL_LINE_CONFIGS,
    )

  private fun createMonitor(): VidLabelingMonitor =
    VidLabelingMonitor(
      rawImpressionUploadStub = rawImpressionUploadStub,
      rawImpressionUploadModelLineStub = rawImpressionUploadModelLineStub,
      dispatchSequencer = createSequencer(),
      dataProviderName = DATA_PROVIDER,
      stalenessThreshold = STALENESS_THRESHOLD,
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
  ) {
    whenever(rawImpressionUploadService.listRawImpressionUploads(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<ListRawImpressionUploadsRequest>(0)
      val uploads =
        when (request.filter.stateInList.firstOrNull()) {
          RawImpressionUpload.State.ACTIVE -> active
          RawImpressionUpload.State.CREATED -> created
          RawImpressionUpload.State.FAILED -> failed
          else -> emptyList()
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

  private fun upload(id: String, state: RawImpressionUpload.State, createdAt: Instant) =
    rawImpressionUpload {
      name = "$DATA_PROVIDER/rawImpressionUploads/$id"
      this.state = state
      createTime = Timestamps.fromMillis(createdAt.toEpochMilli())
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
    whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
    stubMarkTransitions()

    val result = createMonitor().run()

    assertThat(result.dispatchedUpload).isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1")
    assertThat(result.hasIssues).isFalse()
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

      val result = createMonitor().run()

      assertThat(result.dispatchedUpload).isNull()
      assertThat(result.queuedUploads).isEqualTo(1)
      verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
    }

  @Test
  fun `run reports a dispatch error and sets the gauge when the sequencer throws`() = runBlocking {
    // The sequencer's first RPC (listUploads) fails, so dispatchNext() throws.
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenThrow(StatusRuntimeException(Status.UNAVAILABLE))

    val result = createMonitor().run()

    assertThat(result.dispatchError).isTrue()
    assertThat(result.hasIssues).isTrue()
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

    val result = createMonitor().run()

    assertThat(result.stuckUploads)
      .containsExactly("$DATA_PROVIDER/rawImpressionUploads/active-stuck")
    assertThat(result.hasIssues).isTrue()
  }

  @Test
  fun `run does not flag a recent ACTIVE upload as stuck`() = runBlocking {
    stubUploads(active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)))
    stubModelLines()

    val result = createMonitor().run()

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

    val result = createMonitor().run()

    assertThat(result.failedModelLines)
      .containsExactly("$DATA_PROVIDER/rawImpressionUploads/active-1/modelLines/ml1")
    assertThat(result.hasIssues).isTrue()
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

    val result = createMonitor().run()

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

    val result = createMonitor().run()

    assertThat(result.stuckUploads).isEmpty()
    assertThat(result.failedModelLines)
      .containsExactly("$DATA_PROVIDER/rawImpressionUploads/failed-1/modelLines/ml1")
    assertThat(result.hasIssues).isTrue()
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edp123"
    private const val DISPATCH_ERRORS_METRIC = "edpa.vid_labeling_monitor.dispatch_errors"
    private const val MODEL_SUITE = "modelProviders/mp1/modelSuites/ms1"
    private const val MODEL_LINE = "$MODEL_SUITE/modelLines/ml1"
    private const val MODEL_RELEASE = "$MODEL_SUITE/modelReleases/mr1"
    private const val MODEL_BLOB_PATH = "gs://models/vid-model-v1.pb"
    private const val QUEUE_NAME = "queues/vid-labeler-queue"
    private const val NUMBER_OF_SHARDS = 2

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
            labelerInputFieldMapping["age"] = "user_age"
            labelerInputFieldMapping["gender"] = "user_gender"
          }
      )
  }
}
