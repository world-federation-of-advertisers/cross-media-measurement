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

package org.wfanet.measurement.edpaggregator.vidrankbuilder

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.markRankerJobSucceededResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.vidRankBuilderParams
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.storage.StorageClient

private const val DATA_PROVIDER = "dataProviders/dp"
private const val UPLOAD = "dataProviders/dp/rawImpressionUploads/up1"
private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml1"
private const val RANKER_JOB = "dataProviders/dp/rawImpressionUploads/up1/rankerJobs/rj7"
private const val PARENT_NAME =
  "dataProviders/dp/rawImpressionUploads/up1/rawImpressionUploadModelLines/rl1"
private const val QUEUE = "queues/vid-labeler"

/**
 * Unit tests for [VidRankBuilderApp.runWork] — the Phase-1 TEE adapter that unpacks
 * [VidRankBuilderParams], validates them, resolves the per-WorkItem dependencies, and delegates to
 * [VidRankBuilder].
 *
 * Ranking itself is exercised by `VidRankBuilderTest`/`SubpoolRankerTest`; here
 * `subpool_map_blob_uris` is left empty so `runWork` skips ranking and we can focus on (1) param
 * validation and (2) the `max_file_batch_size_bytes` resolution + memoized-`VidLabelerParams`
 * template assembly that this class owns.
 */
@RunWith(JUnit4::class)
class VidRankBuilderAppTest {

  private fun rankerJobsMock(isLastJob: Boolean = true): RankerJobServiceCoroutineStub = mock {
    onBlocking { getRankerJob(any(), any()) } doReturn
      rankerJob {
        name = RANKER_JOB
        state = RankerJob.State.CREATED
        etag = "etag-1"
      }
    onBlocking { markRankerJobSucceeded(any(), any()) } doReturn
      markRankerJobSucceededResponse {
        rankerJob = rankerJob { name = RANKER_JOB }
        this.isLastJob = isLastJob
      }
    onBlocking { markRankerJobFailed(any(), any()) } doReturn rankerJob { name = RANKER_JOB }
  }

  private fun modelLinesMock(): RawImpressionUploadModelLineServiceCoroutineStub = mock {
    onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
      listRawImpressionUploadModelLinesResponse {
        rawImpressionUploadModelLines += rawImpressionUploadModelLine {
          name = PARENT_NAME
          cmmsModelLine = MODEL_LINE
          state = RawImpressionUploadModelLine.State.RANKING
        }
      }
    onBlocking { markRawImpressionUploadModelLineLabeling(any(), any()) } doReturn
      rawImpressionUploadModelLine { name = PARENT_NAME }
  }

  private fun vidLabelingJobsMock(): VidLabelingJobServiceCoroutineStub = mock {
    onBlocking { batchCreateVidLabelingJobs(any(), any()) } doAnswer
      { invocation ->
        val request = invocation.getArgument<BatchCreateVidLabelingJobsRequest>(0)
        batchCreateVidLabelingJobsResponse {
          request.requestsList.forEachIndexed { i, createRequest ->
            vidLabelingJobs +=
              createRequest.vidLabelingJob.copy { name = "${request.parent}/vidLabelingJobs/job$i" }
          }
        }
      }
  }

  /** Three 100-byte files, mirroring `VidRankBuilderTest`. */
  private fun filesMock(): RawImpressionUploadFileServiceCoroutineStub = mock {
    onBlocking { listRawImpressionUploadFiles(any(), any()) } doReturn
      listRawImpressionUploadFilesResponse {
        rawImpressionUploadFiles += rawImpressionUploadFile {
          name = "$UPLOAD/files/0"
          sizeBytes = 100
        }
        rawImpressionUploadFiles += rawImpressionUploadFile {
          name = "$UPLOAD/files/1"
          sizeBytes = 100
        }
        rawImpressionUploadFiles += rawImpressionUploadFile {
          name = "$UPLOAD/files/2"
          sizeBytes = 100
        }
      }
  }

  private fun recordingWorkItems(into: MutableList<CreateWorkItemRequest>): WorkItemsCoroutineStub =
    mock {
      onBlocking { createWorkItem(any(), any()) } doAnswer
        {
          into.add(it.getArgument(0))
          workItem {}
        }
    }

  private fun publishedParams(request: CreateWorkItemRequest): VidLabelerParams =
    request.workItem.workItemParams
      .unpack(WorkItemParams::class.java)
      .appParams
      .unpack(VidLabelerParams::class.java)

  private fun storageParams(prefix: String) =
    VidRankBuilderParamsKt.storageParams {
      gcsProjectId = "test-project"
      blobPrefix = prefix
    }

  /** A fully-populated, valid [VidRankBuilderParams] with no subpools (so ranking is skipped). */
  private fun validParams(): VidRankBuilderParams = vidRankBuilderParams {
    dataProvider = DATA_PROVIDER
    encryptedSubpoolMapsDek = encryptedDek {}
    subpoolMapStorageParams = storageParams("subpool-map")
    vidRankMapStorageParams = storageParams("vid-rank-map")
    rawImpressionStorageParams = storageParams("raw-impressions")
    vidLabeledImpressionsStorageParams = storageParams("vid-labeled")
    rawImpressionUpload = UPLOAD
    modelLine = MODEL_LINE
    modelBlobPath = "model/blob"
    rankerJob = RANKER_JOB
    totalShards = 1
    labelerInputFieldMapping.put("event_id.id", "raw_event_id")
    eventTemplateFieldMapping.put("banner_ad.viewable", "raw_viewable")
    maxFileBatchSizeBytes = 1_000_000_000
    // subpool_map_blob_uris intentionally empty -> runWork skips ranking.
  }

  private fun buildMessage(params: VidRankBuilderParams): Any =
    workItemParams { appParams = params.pack() }.pack()

  private fun createApp(
    kmsClients: Map<String, KmsClient> = mapOf(DATA_PROVIDER to mock<KmsClient>()),
    retentionDays: Map<String, Int> = mapOf(DATA_PROVIDER to 30),
    rankerJobsStub: RankerJobServiceCoroutineStub = rankerJobsMock(),
    modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub = modelLinesMock(),
    vidLabelingJobsStub: VidLabelingJobServiceCoroutineStub = vidLabelingJobsMock(),
    filesStub: RawImpressionUploadFileServiceCoroutineStub = filesMock(),
    workItemsStub: WorkItemsCoroutineStub = mock(),
  ): VidRankBuilderApp =
    VidRankBuilderApp(
      subscriptionId = "test-subscription",
      queueSubscriber = mock<QueueSubscriber>(),
      parser = WorkItem.parser(),
      workItemsClient = workItemsStub,
      workItemAttemptsClient = mock<WorkItemAttemptsCoroutineStub>(),
      kmsClients = kmsClients,
      retentionDaysByDataProvider = retentionDays,
      getSubpoolMapStorageConfig = { StorageConfig(projectId = it.gcsProjectId) },
      getVidRankMapStorageConfig = { StorageConfig(projectId = it.gcsProjectId) },
      rankerJobsStub = rankerJobsStub,
      rankIndexBlobsStub = mock<RankIndexBlobServiceCoroutineStub>(),
      rawImpressionUploadModelLinesStub = modelLinesStub,
      vidLabelingJobsStub = vidLabelingJobsStub,
      rawImpressionUploadFilesStub = filesStub,
      vidLabelerQueue = QUEUE,
      buildSubpoolMapStorageClient = { mock<StorageClient>() },
      buildVidRankMapStorageClient = { mock<StorageClient>() },
      getVidRankMapKekUri = { "kek-uri" },
    )

  @Test
  fun `runWork rejects empty data_provider`() = runBlocking {
    val params = validParams().toBuilder().setDataProvider("").build()
    assertFailsWith<IllegalArgumentException> { createApp().runWork(buildMessage(params)) }
    Unit
  }

  @Test
  fun `runWork rejects missing subpool_map_storage_params`() = runBlocking {
    val params = validParams().toBuilder().clearSubpoolMapStorageParams().build()
    assertFailsWith<IllegalArgumentException> { createApp().runWork(buildMessage(params)) }
    Unit
  }

  @Test
  fun `runWork rejects missing vid_rank_map_storage_params`() = runBlocking {
    val params = validParams().toBuilder().clearVidRankMapStorageParams().build()
    assertFailsWith<IllegalArgumentException> { createApp().runWork(buildMessage(params)) }
    Unit
  }

  @Test
  fun `runWork rejects missing encrypted_subpool_maps_dek`() = runBlocking {
    val params = validParams().toBuilder().clearEncryptedSubpoolMapsDek().build()
    assertFailsWith<IllegalArgumentException> { createApp().runWork(buildMessage(params)) }
    Unit
  }

  @Test
  fun `runWork rejects non-positive total_shards`() = runBlocking {
    val params = validParams().toBuilder().setTotalShards(0).build()
    assertFailsWith<IllegalArgumentException> { createApp().runWork(buildMessage(params)) }
    Unit
  }

  @Test
  fun `runWork fails when no KMS client is configured for the data provider`() = runBlocking {
    assertFailsWith<IllegalArgumentException> {
      createApp(kmsClients = emptyMap()).runWork(buildMessage(validParams()))
    }
    Unit
  }

  @Test
  fun `runWork fails when no retention is configured for the data provider`() = runBlocking {
    assertFailsWith<IllegalArgumentException> {
      createApp(retentionDays = emptyMap()).runWork(buildMessage(validParams()))
    }
    Unit
  }

  @Test
  fun `runWork rejects missing model_blob_path`() = runBlocking {
    val params = validParams().toBuilder().clearModelBlobPath().build()
    assertFailsWith<IllegalArgumentException> { createApp().runWork(buildMessage(params)) }
    Unit
  }

  @Test
  fun `runWork rejects missing raw_impression_storage_params`() = runBlocking {
    val params = validParams().toBuilder().clearRawImpressionStorageParams().build()
    assertFailsWith<IllegalArgumentException> { createApp().runWork(buildMessage(params)) }
    Unit
  }

  @Test
  fun `runWork rejects missing vid_labeled_impressions_storage_params`() = runBlocking {
    val params = validParams().toBuilder().clearVidLabeledImpressionsStorageParams().build()
    assertFailsWith<IllegalArgumentException> { createApp().runWork(buildMessage(params)) }
    Unit
  }

  @Test
  fun `runWork forwards max_file_batch_size_bytes from the params into bin-packing`() =
    runBlocking<Unit> {
      val published = mutableListOf<CreateWorkItemRequest>()
      // 3 files at 100 bytes; a 250-byte cap packs [0,1] then [2] -> two WorkItems.
      val params = validParams().toBuilder().setMaxFileBatchSizeBytes(250).build()

      createApp(workItemsStub = recordingWorkItems(published)).runWork(buildMessage(params))

      assertThat(published).hasSize(2)
      val fileSets =
        published.map { publishedParams(it).memoizedParams.rawImpressionUploadFilesList.toSet() }
      assertThat(fileSets)
        .containsExactly(setOf("$UPLOAD/files/0", "$UPLOAD/files/1"), setOf("$UPLOAD/files/2"))
    }

  @Test
  fun `runWork rejects an unset max_file_batch_size_bytes`() = runBlocking {
    // max_file_batch_size_bytes is REQUIRED (no built-in default); 0/unset must fail at the
    // VidRankBuilder boundary rather than silently picking a cap.
    val params = validParams().toBuilder().setMaxFileBatchSizeBytes(0).build()

    assertFailsWith<IllegalArgumentException> { createApp().runWork(buildMessage(params)) }
    Unit
  }

  @Test
  fun `runWork carries pass-through fields into the memoized VidLabelerParams template`() =
    runBlocking<Unit> {
      val published = mutableListOf<CreateWorkItemRequest>()

      createApp(workItemsStub = recordingWorkItems(published)).runWork(buildMessage(validParams()))

      val labelerParams = publishedParams(published.single())
      assertThat(labelerParams.dataProvider).isEqualTo(DATA_PROVIDER)
      assertThat(labelerParams.memoizedParams.modelLine).isEqualTo(MODEL_LINE)
      assertThat(labelerParams.memoizedParams.modelBlobPath).isEqualTo("model/blob")
      assertThat(labelerParams.memoizedParams.vidLabelingJob).isNotEmpty()
      val modelLineConfig = labelerParams.modelLineConfigsMap.getValue(MODEL_LINE)
      assertThat(modelLineConfig.labelerInputFieldMappingMap)
        .containsExactly("event_id.id", "raw_event_id")
      assertThat(modelLineConfig.eventTemplateFieldMappingMap)
        .containsExactly("banner_ad.viewable", "raw_viewable")
    }
}
