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
import io.grpc.Status
import io.grpc.StatusException
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
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatchFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatchServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionMetadataBatch
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.StorageClient

@RunWith(JUnit4::class)
class VidLabelingDispatcherTest {

  private val workItemsService: WorkItemsGrpcKt.WorkItemsCoroutineImplBase = mockService()
  private val rawImpressionMetadataBatchService:
    RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineImplBase =
    mockService()
  private val rawImpressionMetadataBatchFileService:
    RawImpressionMetadataBatchFileServiceGrpcKt.RawImpressionMetadataBatchFileServiceCoroutineImplBase =
    mockService()
  private val storageClient: StorageClient = mock()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(workItemsService)
    addService(rawImpressionMetadataBatchService)
    addService(rawImpressionMetadataBatchFileService)
  }

  private val workItemsStub by lazy {
    WorkItemsGrpcKt.WorkItemsCoroutineStub(grpcTestServerRule.channel)
  }

  private val rawImpressionMetadataBatchStub by lazy {
    RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  private val rawImpressionMetadataBatchFileStub by lazy {
    RawImpressionMetadataBatchFileServiceGrpcKt.RawImpressionMetadataBatchFileServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  private val vidLabelerParamsTemplate = vidLabelerParams {
    dataProvider = DATA_PROVIDER_NAME
    storageParams =
      VidLabelerParamsKt.storageParams {
        gcsProjectId = "test-project"
        labeledImpressionsBlobPrefix = "gs://output-bucket/labeled"
      }
  }

  private fun createDispatcher(
    batchMaxSizeBytes: Long = BATCH_MAX_SIZE_BYTES
  ): VidLabelingDispatcher {
    return VidLabelingDispatcher(
      storageClient = storageClient,
      workItemsStub = workItemsStub,
      rawImpressionMetadataBatchStub = rawImpressionMetadataBatchStub,
      rawImpressionMetadataBatchFileStub = rawImpressionMetadataBatchFileStub,
      dataProviderName = DATA_PROVIDER_NAME,
      vidLabelerParamsTemplate = vidLabelerParamsTemplate,
      queueName = QUEUE_NAME,
      batchMaxSizeBytes = batchMaxSizeBytes,
    )
  }

  private suspend fun stubBatchCreation() {
    whenever(rawImpressionMetadataBatchService.createRawImpressionMetadataBatch(any())).thenAnswer {
      rawImpressionMetadataBatch {
        name = "$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/${java.util.UUID.randomUUID()}"
      }
    }
    whenever(
        rawImpressionMetadataBatchFileService.batchCreateRawImpressionMetadataBatchFiles(any())
      )
      .thenReturn(BatchCreateRawImpressionMetadataBatchFilesResponse.getDefaultInstance())
  }

  private fun createMockBlob(key: String, size: Long): StorageClient.Blob {
    val blob: StorageClient.Blob = mock()
    whenever(blob.blobKey).thenReturn(key)
    whenever(blob.size).thenReturn(size)
    return blob
  }

  @Test
  fun `dispatch with empty directory creates no work items`() = runBlocking {
    whenever(storageClient.listBlobs(any())).thenReturn(emptyFlow())

    val dispatcher = createDispatcher()
    dispatcher.dispatch(DONE_BLOB_PATH)

    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
  }

  @Test
  fun `dispatch with single file creates single work item`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet", 1000L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubBatchCreation()
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    val dispatcher = createDispatcher()
    dispatcher.dispatch(DONE_BLOB_PATH)

    verifyBlocking(rawImpressionMetadataBatchService, times(1)) {
      createRawImpressionMetadataBatch(any())
    }

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(1)) { createWorkItem(requestCaptor.capture()) }

    val request = requestCaptor.firstValue
    assertThat(request.workItemId).startsWith("vid-labeling-")
    assertThat(request.workItem.queue).isEqualTo(QUEUE_NAME)

    val workItemParams = request.workItem.workItemParams.unpack(WorkItemParams::class.java)
    val vidLabelerParams = workItemParams.appParams.unpack(VidLabelerParams::class.java)
    assertThat(vidLabelerParams.dataProvider).isEqualTo(DATA_PROVIDER_NAME)
    assertThat(vidLabelerParams.rawImpressionMetadataBatch)
      .startsWith("$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/")
  }

  @Test
  fun `dispatch with default batch max size creates single work item for all files`() =
    runBlocking {
      val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet", 1000L)
      val blob2 = createMockBlob("$FOLDER_PREFIX/file2.parquet", 2000L)
      val blob3 = createMockBlob("$FOLDER_PREFIX/file3.parquet", 3000L)
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, blob2, blob3))
      stubBatchCreation()
      whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

      val dispatcher = createDispatcher()
      dispatcher.dispatch(DONE_BLOB_PATH)

      verifyBlocking(rawImpressionMetadataBatchService, times(1)) {
        createRawImpressionMetadataBatch(any())
      }

      val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsService, times(1)) { createWorkItem(requestCaptor.capture()) }

      val workItemParams =
        requestCaptor.firstValue.workItem.workItemParams.unpack(WorkItemParams::class.java)
      val vidLabelerParams = workItemParams.appParams.unpack(VidLabelerParams::class.java)
      assertThat(vidLabelerParams.rawImpressionMetadataBatch)
        .startsWith("$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/")
    }

  @Test
  fun `dispatch with batch max size partitions files into multiple work items`() = runBlocking {
    val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet", 400L)
    val blob2 = createMockBlob("$FOLDER_PREFIX/file2.parquet", 400L)
    val blob3 = createMockBlob("$FOLDER_PREFIX/file3.parquet", 400L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, blob2, blob3))
    stubBatchCreation()
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    // Batch max size of 900 bytes: BFD packs file1+file2 into one batch, file3 into another.
    val dispatcher = createDispatcher(batchMaxSizeBytes = 900L)
    dispatcher.dispatch(DONE_BLOB_PATH)

    verifyBlocking(rawImpressionMetadataBatchService, times(2)) {
      createRawImpressionMetadataBatch(any())
    }

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(2)) { createWorkItem(requestCaptor.capture()) }

    val batch1Params =
      requestCaptor.allValues[0]
        .workItem
        .workItemParams
        .unpack(WorkItemParams::class.java)
        .appParams
        .unpack(VidLabelerParams::class.java)
    val batch2Params =
      requestCaptor.allValues[1]
        .workItem
        .workItemParams
        .unpack(WorkItemParams::class.java)
        .appParams
        .unpack(VidLabelerParams::class.java)

    assertThat(batch1Params.rawImpressionMetadataBatch)
      .startsWith("$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/")
    assertThat(batch2Params.rawImpressionMetadataBatch)
      .startsWith("$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/")
    assertThat(batch1Params.rawImpressionMetadataBatch)
      .isNotEqualTo(batch2Params.rawImpressionMetadataBatch)
  }

  @Test
  fun `dispatch excludes done marker from file list`() = runBlocking {
    val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet", 1000L)
    val doneBlob = createMockBlob("$FOLDER_PREFIX/done", 0L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, doneBlob))
    stubBatchCreation()
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    val dispatcher = createDispatcher()
    dispatcher.dispatch(DONE_BLOB_PATH)

    verifyBlocking(rawImpressionMetadataBatchService, times(1)) {
      createRawImpressionMetadataBatch(any())
    }

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(1)) { createWorkItem(requestCaptor.capture()) }

    val workItemParams =
      requestCaptor.firstValue.workItem.workItemParams.unpack(WorkItemParams::class.java)
    val vidLabelerParams = workItemParams.appParams.unpack(VidLabelerParams::class.java)
    assertThat(vidLabelerParams.rawImpressionMetadataBatch)
      .startsWith("$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/")
  }

  @Test
  fun `dispatch uses server-assigned batch ID in work item ID`() = runBlocking {
    val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet", 1000L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1))
    whenever(rawImpressionMetadataBatchService.createRawImpressionMetadataBatch(any()))
      .thenReturn(
        rawImpressionMetadataBatch {
          name = "$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/server-assigned-id"
        }
      )
    whenever(
        rawImpressionMetadataBatchFileService.batchCreateRawImpressionMetadataBatchFiles(any())
      )
      .thenReturn(BatchCreateRawImpressionMetadataBatchFilesResponse.getDefaultInstance())
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    val dispatcher = createDispatcher()
    dispatcher.dispatch(DONE_BLOB_PATH)

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(1)) { createWorkItem(requestCaptor.capture()) }
    assertThat(requestCaptor.firstValue.workItemId)
      .isEqualTo("vid-labeling-$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/server-assigned-id")
  }

  @Test
  fun `dispatch packs files efficiently using best-fit decreasing`() = runBlocking {
    val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet", 400L)
    val blob2 = createMockBlob("$FOLDER_PREFIX/file2.parquet", 400L)
    val blob3 = createMockBlob("$FOLDER_PREFIX/file3.parquet", 600L)
    val blob4 = createMockBlob("$FOLDER_PREFIX/file4.parquet", 600L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, blob2, blob3, blob4))
    stubBatchCreation()
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    // BFD with max 1000: sorts [600, 600, 400, 400], packs (600+400), (600+400).
    val dispatcher = createDispatcher(batchMaxSizeBytes = 1000L)
    dispatcher.dispatch(DONE_BLOB_PATH)

    verifyBlocking(rawImpressionMetadataBatchService, times(2)) {
      createRawImpressionMetadataBatch(any())
    }

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(2)) { createWorkItem(requestCaptor.capture()) }

    val batch1Params =
      requestCaptor.allValues[0]
        .workItem
        .workItemParams
        .unpack(WorkItemParams::class.java)
        .appParams
        .unpack(VidLabelerParams::class.java)
    val batch2Params =
      requestCaptor.allValues[1]
        .workItem
        .workItemParams
        .unpack(WorkItemParams::class.java)
        .appParams
        .unpack(VidLabelerParams::class.java)

    // Each batch should have a unique resource name.
    assertThat(batch1Params.rawImpressionMetadataBatch)
      .startsWith("$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/")
    assertThat(batch2Params.rawImpressionMetadataBatch)
      .startsWith("$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/")
    assertThat(batch1Params.rawImpressionMetadataBatch)
      .isNotEqualTo(batch2Params.rawImpressionMetadataBatch)
  }

  @Test
  fun `dispatch places oversized file in its own batch`() = runBlocking {
    val normalBlob = createMockBlob("$FOLDER_PREFIX/normal.parquet", 400L)
    val oversizedBlob = createMockBlob("$FOLDER_PREFIX/oversized.parquet", 1500L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(normalBlob, oversizedBlob))
    stubBatchCreation()
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    val dispatcher = createDispatcher(batchMaxSizeBytes = 1000L)
    dispatcher.dispatch(DONE_BLOB_PATH)

    verifyBlocking(rawImpressionMetadataBatchService, times(2)) {
      createRawImpressionMetadataBatch(any())
    }

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(2)) { createWorkItem(requestCaptor.capture()) }

    // Oversized batches come first in the output.
    val oversizedBatchParams =
      requestCaptor.allValues[0]
        .workItem
        .workItemParams
        .unpack(WorkItemParams::class.java)
        .appParams
        .unpack(VidLabelerParams::class.java)
    val normalBatchParams =
      requestCaptor.allValues[1]
        .workItem
        .workItemParams
        .unpack(WorkItemParams::class.java)
        .appParams
        .unpack(VidLabelerParams::class.java)

    // Each batch should have a unique resource name.
    assertThat(oversizedBatchParams.rawImpressionMetadataBatch)
      .startsWith("$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/")
    assertThat(normalBatchParams.rawImpressionMetadataBatch)
      .startsWith("$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/")
    assertThat(oversizedBatchParams.rawImpressionMetadataBatch)
      .isNotEqualTo(normalBatchParams.rawImpressionMetadataBatch)
  }

  @Test
  fun `dispatch propagates exception on work item creation failure`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet", 1000L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubBatchCreation()
    whenever(workItemsService.createWorkItem(any())).thenAnswer {
      throw StatusException(Status.UNAVAILABLE.withDescription("Service unavailable"))
    }

    val dispatcher = createDispatcher()
    val exception = assertFailsWith<Exception> { dispatcher.dispatch(DONE_BLOB_PATH) }
    assertThat(exception).hasMessageThat().contains("Error creating WorkItem")
    assertThat(exception).hasCauseThat().isInstanceOf(StatusException::class.java)
  }

  @Test
  fun `dispatch with gs scheme constructs correct blob URIs`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet", 1000L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubBatchCreation()
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    val dispatcher = createDispatcher()
    dispatcher.dispatch(GCS_DONE_BLOB_PATH)

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(1)) { createWorkItem(requestCaptor.capture()) }

    val workItemParams =
      requestCaptor.firstValue.workItem.workItemParams.unpack(WorkItemParams::class.java)
    val vidLabelerParams = workItemParams.appParams.unpack(VidLabelerParams::class.java)
    assertThat(vidLabelerParams.rawImpressionMetadataBatch)
      .startsWith("$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/")
  }

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/edp123"
    private const val QUEUE_NAME = "queues/vid-labeler-queue"
    private const val BATCH_MAX_SIZE_BYTES = 10_000_000_000L
    private const val DONE_BLOB_PATH = "file:///test-bucket/edp1/2024-01-15/done"
    private const val GCS_DONE_BLOB_PATH = "gs://test-bucket/edp1/2024-01-15/done"
    private const val FOLDER_PREFIX = "edp1/2024-01-15"
  }
}
