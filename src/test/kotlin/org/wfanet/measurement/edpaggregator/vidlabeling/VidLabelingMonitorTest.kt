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
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import kotlinx.coroutines.runBlocking
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
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelShardKt.modelBlob
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt
import org.wfanet.measurement.api.v2alpha.listModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.listModelShardsResponse
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelShard
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreatePoolAssignmentJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.transportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

@RunWith(JUnit4::class)
class VidLabelingMonitorTest {

  private val rawImpressionUploadService:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase =
    mockService()
  private val rawImpressionUploadModelLineService:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase =
    mockService()
  private val poolAssignmentJobService:
    PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineImplBase =
    mockService()
  private val workItemsService: WorkItemsGrpcKt.WorkItemsCoroutineImplBase = mockService()
  private val modelRolloutsService: ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase =
    mockService()
  private val modelShardsService: ModelShardsGrpcKt.ModelShardsCoroutineImplBase = mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(rawImpressionUploadService)
    addService(rawImpressionUploadModelLineService)
    addService(poolAssignmentJobService)
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
  private val poolAssignmentJobStub by lazy {
    PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub(grpcTestServerRule.channel)
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

  private fun createMonitor(): VidLabelingMonitor =
    VidLabelingMonitor(
      rawImpressionUploadStub = rawImpressionUploadStub,
      rawImpressionUploadModelLineStub = rawImpressionUploadModelLineStub,
      poolAssignmentJobStub = poolAssignmentJobStub,
      workItemsStub = workItemsStub,
      modelRolloutsStub = modelRolloutsStub,
      modelShardsStub = modelShardsStub,
      dataProviderName = DATA_PROVIDER,
      vidLabelerParamsTemplate = VID_LABELER_PARAMS_TEMPLATE,
      queueName = QUEUE_NAME,
      numberOfShards = NUMBER_OF_SHARDS,
      modelLineConfigs = MODEL_LINE_CONFIGS,
      stalenessThreshold = STALENESS_THRESHOLD,
      clock = fixedClock,
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
        when (request.filter.state) {
          RawImpressionUpload.State.ACTIVE -> active
          RawImpressionUpload.State.CREATED -> created
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

  private fun createdModelLine(id: String = "ml1") =
    rawImpressionUploadModelLine {
      name = "$DATA_PROVIDER/rawImpressionUploads/upload-1/modelLines/$id"
      cmmsModelLine = MODEL_LINE
      state = RawImpressionUploadModelLine.State.CREATED
    }

  @Test
  fun `run dispatches the oldest CREATED upload when none are ACTIVE`() = runBlocking {
    stubUploads(created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW)))
    stubModelLines(createdModelLine())
    stubShardResolution(memoized = false)
    whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
    stubMarkTransitions()

    val result = createMonitor().run()

    assertThat(result.dispatchedUpload).isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1")
    assertThat(result.hasIssues).isFalse()
    // Non-memoized line: one WorkItem per shard, then MarkLabeling once.
    verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(any()) }
    verifyBlocking(rawImpressionUploadModelLineService) {
      markRawImpressionUploadModelLineLabeling(any())
    }
  }

  @Test
  fun `run does not dispatch when an upload is ACTIVE`() = runBlocking {
    stubUploads(
      active = listOf(upload("active-1", RawImpressionUpload.State.ACTIVE, FIXED_NOW)),
      created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW)),
    )
    stubModelLines() // Active upload has no failed/listed model lines.

    val result = createMonitor().run()

    assertThat(result.dispatchedUpload).isNull()
    assertThat(result.queuedUploads).isEqualTo(1)
    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
  }

  @Test
  fun `run dispatches a memoized model line via PoolAssignmentJobs`() = runBlocking {
    stubUploads(created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW)))
    stubModelLines(createdModelLine())
    stubShardResolution(memoized = true)
    whenever(poolAssignmentJobService.batchCreatePoolAssignmentJobs(any()))
      .thenReturn(batchCreatePoolAssignmentJobsResponse {})
    stubMarkTransitions()

    val result = createMonitor().run()

    assertThat(result.dispatchedUpload).isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1")
    verifyBlocking(poolAssignmentJobService) { batchCreatePoolAssignmentJobs(any()) }
    verifyBlocking(rawImpressionUploadModelLineService) {
      markRawImpressionUploadModelLinePoolAssigning(any())
    }
    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
  }

  @Test
  fun `run selects the oldest CREATED upload`() = runBlocking<Unit> {
    val older =
      upload("upload-old", RawImpressionUpload.State.CREATED, FIXED_NOW.minusSeconds(3600))
    val newer = upload("upload-new", RawImpressionUpload.State.CREATED, FIXED_NOW)
    stubUploads(created = listOf(newer, older))
    stubModelLines(createdModelLine())
    stubShardResolution(memoized = false)
    whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
    stubMarkTransitions()

    val result = createMonitor().run()

    assertThat(result.dispatchedUpload).isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-old")
    // WorkItem IDs embed the dispatched upload id.
    val captor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(captor.capture()) }
    assertThat(captor.allValues.map { it.workItemId }).containsExactly(
      "vid-labeling-upload-old-ml1-shard-0",
      "vid-labeling-upload-old-ml1-shard-1",
    )
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
  fun `run skips dispatch when the model shard cannot be resolved`() = runBlocking {
    stubUploads(created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW)))
    stubModelLines(createdModelLine())
    // No rollout found -> shard cannot be resolved.
    whenever(modelRolloutsService.listModelRollouts(any())).thenReturn(listModelRolloutsResponse {})

    val result = createMonitor().run()

    // The upload is still considered dispatched (sequencing claimed it), but no work was created.
    assertThat(result.dispatchedUpload).isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1")
    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      markRawImpressionUploadModelLineLabeling(any())
    }
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edp123"
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
