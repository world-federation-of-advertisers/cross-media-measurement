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
import com.google.protobuf.kotlin.unpack
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import io.grpc.StatusException
import java.time.Instant
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
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadModelLineLabelingRequest
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
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

@RunWith(JUnit4::class)
class VidLabelingDispatchSequencerTest {

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
  ) = rawImpressionUploadModelLine {
    name = "$uploadName/modelLines/ml"
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
    etag = ETAG
  }

  @Test
  fun `dispatchNext dispatches the oldest CREATED upload when none are ACTIVE`() = runBlocking {
    stubUploads(created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW)))
    stubModelLines(createdModelLine())
    stubShardResolution(memoized = false)
    whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
    stubMarkTransitions()

    val result = createSequencer().dispatchNext()

    assertThat(result.dispatchedUpload).isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1")
    // Non-memoized line: one WorkItem per shard, then MarkLabeling once.
    verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(any()) }
    verifyBlocking(rawImpressionUploadModelLineService) {
      markRawImpressionUploadModelLineLabeling(any())
    }
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
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      stubMarkTransitions()

      val result = createSequencer().dispatchNext()

      // MODEL_LINE_2 differs from the running MODEL_LINE, so it dispatches in parallel.
      assertThat(result.dispatchedUpload).isEqualTo(created.name)
      verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(any()) }
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
  fun `dispatchNext skips a memoized model line`() = runBlocking {
    // Memoized (Phase-0 SubpoolAssigner) dispatch is out of scope for this PR (see #4062), so a
    // memoized model line is left CREATED and no work is created.
    stubUploads(created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW)))
    stubModelLines(createdModelLine())
    stubShardResolution(memoized = true)

    val result = createSequencer().dispatchNext()

    assertThat(result.dispatchedUpload).isNull()
    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      markRawImpressionUploadModelLineLabeling(any())
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
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      stubMarkTransitions()

      val result = createSequencer().dispatchNext()

      assertThat(result.dispatchedUpload)
        .isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-old")
      // WorkItem IDs embed the dispatched upload id.
      val captor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsService, times(NUMBER_OF_SHARDS)) { createWorkItem(captor.capture()) }
      assertThat(captor.allValues.map { it.workItemId })
        .containsExactly(
          "vid-labeling-upload-old-ml1-shard-0",
          "vid-labeling-upload-old-ml1-shard-1",
        )
      // The WorkItem's VidLabelerParams carries the RawImpressionUpload resource name.
      val vidLabelerParams =
        captor.firstValue.workItem.workItemParams
          .unpack<WorkItemParams>()
          .appParams
          .unpack<VidLabelerParams>()
      assertThat(vidLabelerParams.rawImpressionUpload)
        .isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-old")
    }

  @Test
  fun `dispatchNext passes the model line etag on the Mark request`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = false)
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
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      // Another concurrent dispatcher claimed the model line first: the etag CAS fails with
      // ABORTED.
      whenever(rawImpressionUploadModelLineService.markRawImpressionUploadModelLineLabeling(any()))
        .thenAnswer { throw StatusException(Status.ABORTED.withDescription("etag mismatch")) }

      // dispatchNext must not propagate the lost-race error.
      val result = createSequencer().dispatchNext()

      assertThat(result.dispatchedUpload).isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1")
    }

  @Test
  fun `dispatchNext skips a model line that already advanced with FAILED_PRECONDITION`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = false)
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      whenever(rawImpressionUploadModelLineService.markRawImpressionUploadModelLineLabeling(any()))
        .thenAnswer {
          throw StatusException(Status.FAILED_PRECONDITION.withDescription("already LABELING"))
        }

      val result = createSequencer().dispatchNext()

      assertThat(result.dispatchedUpload).isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1")
    }

  @Test
  fun `dispatchNext tolerates an already-existing WorkItem`() =
    runBlocking<Unit> {
      stubUploads(
        created = listOf(upload("upload-1", RawImpressionUpload.State.CREATED, FIXED_NOW))
      )
      stubModelLines(createdModelLine())
      stubShardResolution(memoized = false)
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
    private const val NUMBER_OF_SHARDS = 2
    private const val ETAG = "W/\"abc123\""

    private val FIXED_NOW: Instant = Instant.parse("2026-06-03T12:00:00Z")

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
          },
        MODEL_LINE_2 to
          VidLabelerParamsKt.modelLineConfig { labelerInputFieldMapping["age"] = "user_age" },
      )
  }
}
