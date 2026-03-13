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
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.StorageClient

@RunWith(JUnit4::class)
class VidLabelingDispatcherTest {

  private val workItemsService: WorkItemsGrpcKt.WorkItemsCoroutineImplBase = mockService()
  private val storageClient: StorageClient = mock()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(workItemsService) }

  private val workItemsStub by lazy {
    WorkItemsGrpcKt.WorkItemsCoroutineStub(grpcTestServerRule.channel)
  }

  private val vidLabelerParamsTemplate = vidLabelerParams {
    dataProvider = DATA_PROVIDER_NAME
    storageParams = VidLabelerParamsKt.storageParams {
      gcsProjectId = "test-project"
      labeledImpressionsBlobPrefix = "gs://output-bucket/labeled"
    }
  }

  private fun createDispatcher(
    batchMaxSizeBytes: Long = BATCH_MAX_SIZE_BYTES,
  ): VidLabelingDispatcher {
    return VidLabelingDispatcher(
      storageClient = storageClient,
      workItemsStub = workItemsStub,
      dataProviderName = DATA_PROVIDER_NAME,
      vidLabelerParamsTemplate = vidLabelerParamsTemplate,
      queueName = QUEUE_NAME,
      batchMaxSizeBytes = batchMaxSizeBytes,
    )
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
    whenever(workItemsService.createWorkItem(any())).thenReturn(
      WorkItem.getDefaultInstance()
    )

    val dispatcher = createDispatcher()
    dispatcher.dispatch(DONE_BLOB_PATH)

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(1)) { createWorkItem(requestCaptor.capture()) }

    val request = requestCaptor.firstValue
    assertThat(request.workItemId).startsWith("vid-labeling-")
    assertThat(request.workItem.queue).isEqualTo(QUEUE_NAME)

    val workItemParams =
      request.workItem.workItemParams.unpack(
        org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams::class
          .java
      )
    val vidLabelerParams = workItemParams.appParams.unpack(VidLabelerParams::class.java)
    assertThat(vidLabelerParams.dataProvider).isEqualTo(DATA_PROVIDER_NAME)
    assertThat(vidLabelerParams.inputBlobUrisList).hasSize(1)
    assertThat(vidLabelerParams.inputBlobUrisList[0]).contains("file1.parquet")
    assertThat(vidLabelerParams.batchIndex).isEqualTo(0L)
  }

  @Test
  fun `dispatch with default batch max size creates single work item for all files`() = runBlocking {
    val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet", 1000L)
    val blob2 = createMockBlob("$FOLDER_PREFIX/file2.parquet", 2000L)
    val blob3 = createMockBlob("$FOLDER_PREFIX/file3.parquet", 3000L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, blob2, blob3))
    whenever(workItemsService.createWorkItem(any())).thenReturn(
      WorkItem.getDefaultInstance()
    )

    val dispatcher = createDispatcher()
    dispatcher.dispatch(DONE_BLOB_PATH)

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(1)) { createWorkItem(requestCaptor.capture()) }

    val workItemParams =
      requestCaptor.firstValue.workItem.workItemParams.unpack(
        org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams::class
          .java
      )
    val vidLabelerParams = workItemParams.appParams.unpack(VidLabelerParams::class.java)
    assertThat(vidLabelerParams.inputBlobUrisList).hasSize(3)
  }

  @Test
  fun `dispatch with batch max size partitions files into multiple work items`() = runBlocking {
    val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet", 400L)
    val blob2 = createMockBlob("$FOLDER_PREFIX/file2.parquet", 400L)
    val blob3 = createMockBlob("$FOLDER_PREFIX/file3.parquet", 400L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, blob2, blob3))
    whenever(workItemsService.createWorkItem(any())).thenReturn(
      WorkItem.getDefaultInstance()
    )

    // Batch max size of 900 bytes: BFD packs file1+file2 into one batch, file3 into another.
    val dispatcher = createDispatcher(batchMaxSizeBytes = 900L)
    dispatcher.dispatch(DONE_BLOB_PATH)

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(2)) { createWorkItem(requestCaptor.capture()) }

    val batch1Params =
      requestCaptor.allValues[0]
        .workItem
        .workItemParams
        .unpack(
          org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams::
            class
            .java
        )
        .appParams
        .unpack(VidLabelerParams::class.java)
    val batch2Params =
      requestCaptor.allValues[1]
        .workItem
        .workItemParams
        .unpack(
          org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams::
            class
            .java
        )
        .appParams
        .unpack(VidLabelerParams::class.java)

    assertThat(batch1Params.inputBlobUrisList).hasSize(2)
    assertThat(batch1Params.batchIndex).isEqualTo(0L)
    assertThat(batch2Params.inputBlobUrisList).hasSize(1)
    assertThat(batch2Params.batchIndex).isEqualTo(1L)
  }

  @Test
  fun `dispatch excludes done marker from file list`() = runBlocking {
    val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet", 1000L)
    val doneBlob = createMockBlob("$FOLDER_PREFIX/done", 0L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, doneBlob))
    whenever(workItemsService.createWorkItem(any())).thenReturn(
      WorkItem.getDefaultInstance()
    )

    val dispatcher = createDispatcher()
    dispatcher.dispatch(DONE_BLOB_PATH)

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(1)) { createWorkItem(requestCaptor.capture()) }

    val workItemParams =
      requestCaptor.firstValue.workItem.workItemParams.unpack(
        org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams::class
          .java
      )
    val vidLabelerParams = workItemParams.appParams.unpack(VidLabelerParams::class.java)
    assertThat(vidLabelerParams.inputBlobUrisList).hasSize(1)
    assertThat(vidLabelerParams.inputBlobUrisList[0]).contains("file1.parquet")
  }

  @Test
  fun `dispatch produces deterministic work item IDs for same input`() = runBlocking {
    val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet", 1000L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1))
    whenever(workItemsService.createWorkItem(any())).thenReturn(
      WorkItem.getDefaultInstance()
    )

    val dispatcher = createDispatcher()
    dispatcher.dispatch(DONE_BLOB_PATH)

    val requestCaptor1 = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(1)) { createWorkItem(requestCaptor1.capture()) }
    val firstWorkItemId = requestCaptor1.firstValue.workItemId

    // Dispatch again with the same files.
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1))
    dispatcher.dispatch(DONE_BLOB_PATH)

    val requestCaptor2 = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(2)) { createWorkItem(requestCaptor2.capture()) }
    val secondWorkItemId = requestCaptor2.allValues[1].workItemId

    assertThat(firstWorkItemId).isEqualTo(secondWorkItemId)
  }

  @Test
  fun `dispatch packs files efficiently using best-fit decreasing`() = runBlocking {
    val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet", 400L)
    val blob2 = createMockBlob("$FOLDER_PREFIX/file2.parquet", 400L)
    val blob3 = createMockBlob("$FOLDER_PREFIX/file3.parquet", 600L)
    val blob4 = createMockBlob("$FOLDER_PREFIX/file4.parquet", 600L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, blob2, blob3, blob4))
    whenever(workItemsService.createWorkItem(any())).thenReturn(
      WorkItem.getDefaultInstance()
    )

    // BFD with max 1000: sorts [600, 600, 400, 400], packs (600+400), (600+400).
    val dispatcher = createDispatcher(batchMaxSizeBytes = 1000L)
    dispatcher.dispatch(DONE_BLOB_PATH)

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(2)) { createWorkItem(requestCaptor.capture()) }

    val batch1Params =
      requestCaptor.allValues[0]
        .workItem
        .workItemParams
        .unpack(
          org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams::
            class
            .java
        )
        .appParams
        .unpack(VidLabelerParams::class.java)
    val batch2Params =
      requestCaptor.allValues[1]
        .workItem
        .workItemParams
        .unpack(
          org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams::
            class
            .java
        )
        .appParams
        .unpack(VidLabelerParams::class.java)

    // Each batch should contain exactly 2 files (one 600 + one 400).
    assertThat(batch1Params.inputBlobUrisList).hasSize(2)
    assertThat(batch2Params.inputBlobUrisList).hasSize(2)
  }

  @Test
  fun `dispatch places oversized file in its own batch`() = runBlocking {
    val normalBlob = createMockBlob("$FOLDER_PREFIX/normal.parquet", 400L)
    val oversizedBlob = createMockBlob("$FOLDER_PREFIX/oversized.parquet", 1500L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(normalBlob, oversizedBlob))
    whenever(workItemsService.createWorkItem(any())).thenReturn(
      WorkItem.getDefaultInstance()
    )

    val dispatcher = createDispatcher(batchMaxSizeBytes = 1000L)
    dispatcher.dispatch(DONE_BLOB_PATH)

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(2)) { createWorkItem(requestCaptor.capture()) }

    // Oversized batches come first in the output.
    val oversizedBatchParams =
      requestCaptor.allValues[0]
        .workItem
        .workItemParams
        .unpack(
          org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams::
            class
            .java
        )
        .appParams
        .unpack(VidLabelerParams::class.java)
    val normalBatchParams =
      requestCaptor.allValues[1]
        .workItem
        .workItemParams
        .unpack(
          org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams::
            class
            .java
        )
        .appParams
        .unpack(VidLabelerParams::class.java)

    // Oversized file is alone in its batch.
    assertThat(oversizedBatchParams.inputBlobUrisList).hasSize(1)
    assertThat(oversizedBatchParams.inputBlobUrisList[0]).contains("oversized.parquet")
    // Normal file is in its own batch.
    assertThat(normalBatchParams.inputBlobUrisList).hasSize(1)
    assertThat(normalBatchParams.inputBlobUrisList[0]).contains("normal.parquet")
  }

  @Test
  fun `dispatch with unsupported URI scheme throws exception`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet", 1000L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))

    val dispatcher = createDispatcher()
    val exception =
      assertFailsWith<IllegalArgumentException> {
        dispatcher.dispatch("s3://test-bucket/$FOLDER_PREFIX/done")
      }
    assertThat(exception).hasMessageThat().contains("Unsupported scheme")
  }

  @Test
  fun `dispatch propagates exception on work item creation failure`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet", 1000L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    whenever(workItemsService.createWorkItem(any()))
      .thenThrow(StatusException(Status.UNAVAILABLE.withDescription("Service unavailable")))

    val dispatcher = createDispatcher()
    val exception =
      assertFailsWith<Exception> { dispatcher.dispatch(DONE_BLOB_PATH) }
    assertThat(exception).hasMessageThat().contains("Error creating WorkItem")
    assertThat(exception).hasCauseThat().isInstanceOf(StatusException::class.java)
  }

  @Test
  fun `dispatch with gs scheme constructs correct blob URIs`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet", 1000L)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    whenever(workItemsService.createWorkItem(any())).thenReturn(
      WorkItem.getDefaultInstance()
    )

    val dispatcher = createDispatcher()
    dispatcher.dispatch(GCS_DONE_BLOB_PATH)

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(1)) { createWorkItem(requestCaptor.capture()) }

    val workItemParams =
      requestCaptor.firstValue.workItem.workItemParams.unpack(
        org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams::class
          .java
      )
    val vidLabelerParams = workItemParams.appParams.unpack(VidLabelerParams::class.java)
    assertThat(vidLabelerParams.inputBlobUrisList[0])
      .isEqualTo("gs://test-bucket/$FOLDER_PREFIX/file1.parquet")
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
