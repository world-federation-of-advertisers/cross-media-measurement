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
import io.grpc.StatusException
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt
import org.wfanet.measurement.api.v2alpha.listModelLinesResponse
import org.wfanet.measurement.api.v2alpha.listModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.listModelShardsResponse
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelShard
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.StorageClient

@RunWith(JUnit4::class)
class VidLabelingDispatcherTest {

  private val workItemsService: WorkItemsGrpcKt.WorkItemsCoroutineImplBase = mockService()
  private val modelLinesService: ModelLinesGrpcKt.ModelLinesCoroutineImplBase = mockService()
  private val modelRolloutsService: ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase =
    mockService()
  private val modelShardsService: ModelShardsGrpcKt.ModelShardsCoroutineImplBase = mockService()
  private val storageClient: StorageClient = mock()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(workItemsService)
    addService(modelLinesService)
    addService(modelRolloutsService)
    addService(modelShardsService)
  }

  private val workItemsStub by lazy {
    WorkItemsGrpcKt.WorkItemsCoroutineStub(grpcTestServerRule.channel)
  }

  private val modelLinesStub by lazy {
    ModelLinesGrpcKt.ModelLinesCoroutineStub(grpcTestServerRule.channel)
  }

  private val modelRolloutsStub by lazy {
    ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub(grpcTestServerRule.channel)
  }

  private val modelShardsStub by lazy {
    ModelShardsGrpcKt.ModelShardsCoroutineStub(grpcTestServerRule.channel)
  }

  private val vidLabelerParamsTemplate = vidLabelerParams {
    dataProvider = DATA_PROVIDER_NAME
    vidLabeledImpressionsStorageParams =
      VidLabelerParamsKt.storageParams {
        gcsProjectId = "test-project"
        impressionsBlobPrefix = "gs://output-bucket/labeled"
      }
    rawImpressionsStorageParams =
      VidLabelerParamsKt.storageParams {
        gcsProjectId = "test-project"
        impressionsBlobPrefix = "gs://input-bucket/raw"
      }
  }

  private val fixedClock: Clock = Clock.fixed(FIXED_NOW, ZoneId.of("UTC"))

  private fun createDispatcher(
    numberOfShards: Int = DEFAULT_NUMBER_OF_SHARDS,
    overrideModelLines: List<String> = emptyList(),
    modelLineConfigs: Map<String, VidLabelerParams.ModelLineConfig> = DEFAULT_MODEL_LINE_CONFIGS,
  ): VidLabelingDispatcher {
    return VidLabelingDispatcher(
      storageClient = storageClient,
      workItemsStub = workItemsStub,
      modelLinesStub = modelLinesStub,
      modelRolloutsStub = modelRolloutsStub,
      modelShardsStub = modelShardsStub,
      dataProviderName = DATA_PROVIDER_NAME,
      vidLabelerParamsTemplate = vidLabelerParamsTemplate,
      queueName = QUEUE_NAME,
      numberOfShards = numberOfShards,
      modelSuiteName = MODEL_SUITE_NAME,
      overrideModelLines = overrideModelLines,
      modelLineConfigs = modelLineConfigs,
      clock = fixedClock,
    )
  }

  private fun createMockBlob(key: String): StorageClient.Blob {
    val blob: StorageClient.Blob = mock()
    whenever(blob.blobKey).thenReturn(key)
    return blob
  }

  private suspend fun stubFullResolutionChain(vararg modelLineNames: String) {
    whenever(modelLinesService.listModelLines(any())).thenReturn(
      listModelLinesResponse {
        modelLines +=
          modelLineNames.map { name ->
            modelLine {
              this.name = name
              type = ModelLine.Type.PROD
              activeStartTime = Timestamps.fromMillis(FIXED_NOW.toEpochMilli() - 86400000)
              activeEndTime = Timestamps.fromMillis(FIXED_NOW.toEpochMilli() + 86400000)
            }
          }
      }
    )

    whenever(modelRolloutsService.listModelRollouts(any())).thenReturn(
      listModelRolloutsResponse {
        modelRollouts += modelRollout { modelRelease = MODEL_RELEASE_NAME }
      }
    )

    whenever(modelShardsService.listModelShards(any())).thenReturn(
      listModelShardsResponse {
        modelShards += modelShard {
          name = "$DATA_PROVIDER_NAME/modelShards/ms1"
          modelRelease = MODEL_RELEASE_NAME
          modelBlob =
            org.wfanet.measurement.api.v2alpha.ModelShardKt.modelBlob {
              modelBlobPath = MODEL_BLOB_PATH
            }
        }
      }
    )
  }

  private suspend fun stubOverrideResolutionChain() {
    whenever(modelRolloutsService.listModelRollouts(any())).thenReturn(
      listModelRolloutsResponse {
        modelRollouts += modelRollout { modelRelease = MODEL_RELEASE_NAME }
      }
    )

    whenever(modelShardsService.listModelShards(any())).thenReturn(
      listModelShardsResponse {
        modelShards += modelShard {
          name = "$DATA_PROVIDER_NAME/modelShards/ms1"
          modelRelease = MODEL_RELEASE_NAME
          modelBlob =
            org.wfanet.measurement.api.v2alpha.ModelShardKt.modelBlob {
              modelBlobPath = MODEL_BLOB_PATH
            }
        }
      }
    )
  }

  @Test
  fun `dispatch with empty directory creates no work items`() = runBlocking {
    whenever(storageClient.listBlobs(any())).thenReturn(emptyFlow())

    val dispatcher = createDispatcher()
    dispatcher.dispatch(DONE_BLOB_PATH)

    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
    verifyBlocking(modelLinesService, never()) { listModelLines(any()) }
  }

  @Test
  fun `dispatch creates N work items per active model line`() = runBlocking {
    val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1))
    stubFullResolutionChain(MODEL_LINE_1, MODEL_LINE_2)
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    val dispatcher = createDispatcher(numberOfShards = 3)
    dispatcher.dispatch(DONE_BLOB_PATH)

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(6)) { createWorkItem(requestCaptor.capture()) }

    val workItemIds = requestCaptor.allValues.map { it.workItemId }
    assertThat(workItemIds).containsExactly(
      "vid-labeling-ml1-shard-0",
      "vid-labeling-ml1-shard-1",
      "vid-labeling-ml1-shard-2",
      "vid-labeling-ml2-shard-0",
      "vid-labeling-ml2-shard-1",
      "vid-labeling-ml2-shard-2",
    )
  }

  @Test
  fun `dispatch with override model lines skips ListModelLines API`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubOverrideResolutionChain()
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    val dispatcher =
      createDispatcher(
        numberOfShards = 2,
        overrideModelLines = listOf(MODEL_LINE_1),
      )
    dispatcher.dispatch(DONE_BLOB_PATH)

    verifyBlocking(modelLinesService, never()) { listModelLines(any()) }

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(2)) { createWorkItem(requestCaptor.capture()) }
  }

  @Test
  fun `dispatch with no active model lines creates no work items`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    whenever(modelLinesService.listModelLines(any())).thenReturn(
      listModelLinesResponse {}
    )

    val dispatcher = createDispatcher()
    dispatcher.dispatch(DONE_BLOB_PATH)

    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
  }

  @Test
  fun `dispatch sets shard index and model blob path in work item params`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubFullResolutionChain(MODEL_LINE_1)
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    val dispatcher = createDispatcher(numberOfShards = 2)
    dispatcher.dispatch(DONE_BLOB_PATH)

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(2)) { createWorkItem(requestCaptor.capture()) }

    val params0 =
      requestCaptor.allValues[0]
        .workItem
        .workItemParams
        .unpack(WorkItemParams::class.java)
        .appParams
        .unpack(VidLabelerParams::class.java)
    assertThat(params0.shardIndex).isEqualTo(0)
    assertThat(params0.totalShards).isEqualTo(2)
    assertThat(params0.modelBlobPathsMap[MODEL_LINE_1]).isEqualTo(MODEL_BLOB_PATH)

    val params1 =
      requestCaptor.allValues[1]
        .workItem
        .workItemParams
        .unpack(WorkItemParams::class.java)
        .appParams
        .unpack(VidLabelerParams::class.java)
    assertThat(params1.shardIndex).isEqualTo(1)
    assertThat(params1.totalShards).isEqualTo(2)
  }

  @Test
  fun `dispatch excludes done marker from file list`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    val doneBlob = createMockBlob("$FOLDER_PREFIX/done")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob, doneBlob))
    stubFullResolutionChain(MODEL_LINE_1)
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    val dispatcher = createDispatcher(numberOfShards = 1)
    dispatcher.dispatch(DONE_BLOB_PATH)

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(1)) { createWorkItem(requestCaptor.capture()) }
  }

  @Test
  fun `dispatch propagates exception on work item creation failure`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubFullResolutionChain(MODEL_LINE_1)
    whenever(workItemsService.createWorkItem(any())).thenAnswer {
      throw StatusException(Status.UNAVAILABLE.withDescription("Service unavailable"))
    }

    val dispatcher = createDispatcher(numberOfShards = 1)
    val exception = assertFailsWith<Exception> { dispatcher.dispatch(DONE_BLOB_PATH) }
    assertThat(exception).hasMessageThat().contains("Error creating WorkItem")
    assertThat(exception).hasCauseThat().isInstanceOf(StatusException::class.java)
  }

  @Test
  fun `dispatch propagates exception on ListModelLines failure`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    whenever(modelLinesService.listModelLines(any())).thenAnswer {
      throw StatusException(Status.UNAVAILABLE.withDescription("VID Repo unavailable"))
    }

    val dispatcher = createDispatcher()
    val exception = assertFailsWith<Exception> { dispatcher.dispatch(DONE_BLOB_PATH) }
    assertThat(exception).hasMessageThat().contains("Error listing model lines")
    assertThat(exception).hasCauseThat().isInstanceOf(StatusException::class.java)
  }

  @Test
  fun `dispatch skips model line when no rollout found`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    whenever(modelLinesService.listModelLines(any())).thenReturn(
      listModelLinesResponse {
        modelLines += modelLine {
          name = MODEL_LINE_1
          type = ModelLine.Type.PROD
          activeStartTime = Timestamps.fromMillis(FIXED_NOW.toEpochMilli() - 86400000)
          activeEndTime = Timestamps.fromMillis(FIXED_NOW.toEpochMilli() + 86400000)
        }
      }
    )
    whenever(modelRolloutsService.listModelRollouts(any())).thenReturn(
      listModelRolloutsResponse {}
    )

    val dispatcher = createDispatcher(numberOfShards = 1)
    dispatcher.dispatch(DONE_BLOB_PATH)

    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
  }

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/edp123"
    private const val QUEUE_NAME = "queues/vid-labeler-queue"
    private const val MODEL_SUITE_NAME = "modelProviders/mp1/modelSuites/ms1"
    private const val MODEL_LINE_1 = "$MODEL_SUITE_NAME/modelLines/ml1"
    private const val MODEL_LINE_2 = "$MODEL_SUITE_NAME/modelLines/ml2"
    private const val MODEL_RELEASE_NAME = "$MODEL_SUITE_NAME/modelReleases/mr1"
    private const val MODEL_BLOB_PATH = "gs://models/vid-model-v1.pb"
    private const val DEFAULT_NUMBER_OF_SHARDS = 3
    private const val DONE_BLOB_PATH = "file:///test-bucket/edp1/2024-01-15/done"

    private val FIXED_NOW: Instant = Instant.parse("2026-06-03T12:00:00Z")

    private val DEFAULT_MODEL_LINE_CONFIGS: Map<String, VidLabelerParams.ModelLineConfig> =
      mapOf(
        MODEL_LINE_1 to
          VidLabelerParamsKt.modelLineConfig {
            labelerInputFieldMapping["age"] = "user_age"
            labelerInputFieldMapping["gender"] = "user_gender"
          },
        MODEL_LINE_2 to
          VidLabelerParamsKt.modelLineConfig {
            labelerInputFieldMapping["age"] = "user_age"
          },
      )
  }
}
