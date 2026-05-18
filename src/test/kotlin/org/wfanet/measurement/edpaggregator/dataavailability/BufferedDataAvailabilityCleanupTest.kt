/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.io.File
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.BatchDeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.DeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchDeleteImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class BufferedDataAvailabilityCleanupTest {

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/dataProvider123"
    private const val BLOB_URI_PREFIX = "gs://bucket/path/to"
  }

  private fun blobUri(index: Int) = "$BLOB_URI_PREFIX/blob-$index"

  private fun resourceId(index: Int) = "$DATA_PROVIDER_NAME/impressionMetadata/im-$index"

  private val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
          val blobUrisList = request.filter.blobUrisList
          if (blobUrisList.isNotEmpty()) {
            listImpressionMetadataResponse {
              impressionMetadata +=
                blobUrisList.mapNotNull { uri ->
                  val index = uri.substringAfterLast("-").toIntOrNull() ?: return@mapNotNull null
                  impressionMetadata {
                    name = resourceId(index)
                    modelLine = "modelLine1"
                    blobUri = uri
                    interval = interval {
                      startTime = timestamp { seconds = 100 }
                      endTime = timestamp { seconds = 200 }
                    }
                    state = ImpressionMetadata.State.ACTIVE
                  }
                }
            }
          } else {
            val blobUri = request.filter.blobUriPrefix
            val index = blobUri.substringAfterLast("-").toIntOrNull() ?: 1
            listImpressionMetadataResponse {
              impressionMetadata +=
                listOf(
                  impressionMetadata {
                    name = resourceId(index)
                    modelLine = "modelLine1"
                    this.blobUri = blobUri
                    interval = interval {
                      startTime = timestamp { seconds = 100 }
                      endTime = timestamp { seconds = 200 }
                    }
                    state = ImpressionMetadata.State.ACTIVE
                  }
                )
            }
          }
        }
      onBlocking { deleteImpressionMetadata(any<DeleteImpressionMetadataRequest>()) }
        .thenAnswer { ImpressionMetadata.getDefaultInstance() }
      onBlocking { batchDeleteImpressionMetadata(any<BatchDeleteImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<BatchDeleteImpressionMetadataRequest>(0)
          batchDeleteImpressionMetadataResponse {
            impressionMetadata +=
              request.namesList.map {
                impressionMetadata {
                  name = it
                  state = ImpressionMetadata.State.DELETED
                }
              }
          }
        }
    }

  private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub by lazy {
    ImpressionMetadataServiceCoroutineStub(grpcTestServerRule.channel)
  }

  @get:Rule val tempFolder = TemporaryFolder()

  /** Storage client with no blobs — simulates all blobs permanently deleted. */
  private fun emptyStorageClient(): FileSystemStorageClient =
    FileSystemStorageClient(tempFolder.newFolder("empty-${System.nanoTime()}"))

  /**
   * Storage client with specific blob keys present — simulates blobs that still have a live
   * (current) version in GCS.
   */
  private fun storageClientWithLiveBlobs(vararg blobKeys: String): FileSystemStorageClient {
    val root = tempFolder.newFolder("storage-${System.nanoTime()}")
    for (key in blobKeys) {
      val file = File(root, key)
      file.parentFile.mkdirs()
      file.writeText("live-blob")
    }
    return FileSystemStorageClient(root)
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(impressionMetadataServiceMock) }

  private fun createBuffer(
    batchSize: Int = 100,
    flushIntervalSeconds: Long = 300,
    storageClient: StorageClient? = null,
  ): BufferedDataAvailabilityCleanup {
    return BufferedDataAvailabilityCleanup(
      impressionMetadataServiceStub = impressionMetadataStub,
      dataProviderName = DATA_PROVIDER_NAME,
      storageClient = storageClient ?: emptyStorageClient(),
      batchSize = batchSize,
      flushIntervalSeconds = flushIntervalSeconds,
    )
  }

  @Test
  fun `enqueue buffers events without immediate processing`() {
    val buffer = createBuffer(batchSize = 10)

    buffer.enqueue(DeleteEvent(blobUri(1), resourceId(1)))

    assertThat(buffer.pendingCount()).isEqualTo(1)
    verifyBlocking(impressionMetadataServiceMock, never()) { batchDeleteImpressionMetadata(any()) }

    buffer.shutdown()
  }

  @Test
  fun `flush calls single batch delete RPC for multiple events with resource IDs`() {
    val buffer = createBuffer(batchSize = 10)

    for (i in 1..5) {
      buffer.enqueue(DeleteEvent(blobUri(i), resourceId(i)))
    }

    assertThat(buffer.pendingCount()).isEqualTo(5)

    buffer.flush()

    assertThat(buffer.pendingCount()).isEqualTo(0)

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchDeleteImpressionMetadata(captor.capture())
    }
    assertThat(captor.firstValue.parent).isEqualTo(DATA_PROVIDER_NAME)
    assertThat(captor.firstValue.namesList).hasSize(5)

    verifyBlocking(impressionMetadataServiceMock, never()) { deleteImpressionMetadata(any()) }

    buffer.shutdown()
  }

  @Test
  fun `flush batch-resolves blob URIs via single list RPC`() {
    val buffer = createBuffer(batchSize = 10)

    for (i in 1..5) {
      buffer.enqueue(DeleteEvent(blobUri(i), null))
    }

    buffer.flush()

    val listCaptor = argumentCaptor<ListImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      listImpressionMetadata(listCaptor.capture())
    }
    assertThat(listCaptor.firstValue.filter.blobUrisList).hasSize(5)

    val deleteCaptor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchDeleteImpressionMetadata(deleteCaptor.capture())
    }
    assertThat(deleteCaptor.firstValue.namesList).hasSize(5)

    buffer.shutdown()
  }

  @Test
  fun `batch size threshold triggers flush with remaining tail via shutdown`() {
    // batchSize=3 with 5 events: first 3 trigger threshold flush, remaining 2 flushed at shutdown
    val buffer = createBuffer(batchSize = 3)

    for (i in 1..5) {
      buffer.enqueue(DeleteEvent(blobUri(i), resourceId(i)))
    }

    // After enqueuing 5 with batchSize=3, the first batch of 3 was flushed at threshold.
    // Remaining 2 are still pending.
    assertThat(buffer.pendingCount()).isEqualTo(2)

    buffer.shutdown()

    assertThat(buffer.pendingCount()).isEqualTo(0)

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(2)) {
      batchDeleteImpressionMetadata(captor.capture())
    }
    assertThat(captor.allValues[0].namesList).hasSize(3)
    assertThat(captor.allValues[1].namesList).hasSize(2)

    buffer.shutdown()
  }

  @Test
  fun `flush chunks large batches into multiple RPCs of MAX_BATCH_DELETE_SIZE`() {
    // batchSize=250 means threshold flush at 250, which chunks into 3 RPCs (100+100+50)
    // Use 260 to ensure tail of 10 is flushed at shutdown
    val buffer = createBuffer(batchSize = 250)

    for (i in 1..260) {
      buffer.enqueue(DeleteEvent(blobUri(i), resourceId(i)))
    }

    // 250 flushed at threshold (in 3 chunks), 10 remain
    assertThat(buffer.pendingCount()).isEqualTo(10)

    buffer.shutdown()

    assertThat(buffer.pendingCount()).isEqualTo(0)

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(4)) {
      batchDeleteImpressionMetadata(captor.capture())
    }

    val allValues = captor.allValues
    assertThat(allValues[0].namesList).hasSize(100)
    assertThat(allValues[1].namesList).hasSize(100)
    assertThat(allValues[2].namesList).hasSize(50)
    assertThat(allValues[3].namesList).hasSize(10)

    buffer.shutdown()
  }

  @Test
  fun `shutdown flushes remaining events via batch RPC`() {
    val buffer = createBuffer(batchSize = 10)

    for (i in 1..5) {
      buffer.enqueue(DeleteEvent(blobUri(i), resourceId(i)))
    }

    assertThat(buffer.pendingCount()).isEqualTo(5)

    buffer.shutdown()

    assertThat(buffer.pendingCount()).isEqualTo(0)
    verifyBlocking(impressionMetadataServiceMock, times(1)) { batchDeleteImpressionMetadata(any()) }
  }

  @Test
  fun `skips events with no matching impression metadata`() {
    wheneverBlocking { impressionMetadataServiceMock.listImpressionMetadata(any()) }
      .thenReturn(listImpressionMetadataResponse {})

    val buffer = createBuffer()

    buffer.enqueue(DeleteEvent(blobUri(9999), null))
    buffer.enqueue(DeleteEvent(blobUri(1), resourceId(1)))

    buffer.flush()

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchDeleteImpressionMetadata(captor.capture())
    }
    assertThat(captor.firstValue.namesList).containsExactly(resourceId(1))

    buffer.shutdown()
  }

  @Test
  fun `periodic timer flushes remaining events including tail`() {
    // batchSize=3 with 5 events: 3 flushed at threshold, 2 flushed by timer
    val buffer = createBuffer(batchSize = 3, flushIntervalSeconds = 1)

    for (i in 1..5) {
      buffer.enqueue(DeleteEvent(blobUri(i), resourceId(i)))
    }

    // 3 were flushed at threshold, 2 remain for timer
    assertThat(buffer.pendingCount()).isEqualTo(2)

    Thread.sleep(2500)

    assertThat(buffer.pendingCount()).isEqualTo(0)

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(2)) {
      batchDeleteImpressionMetadata(captor.capture())
    }
    assertThat(captor.allValues[0].namesList).hasSize(3)
    assertThat(captor.allValues[1].namesList).hasSize(2)

    buffer.shutdown()
  }

  @Test
  fun `skips events whose blobs still have a live version`() {
    // blob-1 still has a live version; blob-2 and blob-3 are fully deleted
    val sc = storageClientWithLiveBlobs("path/to/blob-1")
    val buffer = createBuffer(batchSize = 5, storageClient = sc)

    for (i in 1..3) {
      buffer.enqueue(DeleteEvent(blobUri(i), resourceId(i)))
    }

    buffer.flush()

    assertThat(buffer.pendingCount()).isEqualTo(0)

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchDeleteImpressionMetadata(captor.capture())
    }
    // blob-1 is skipped (live version exists), blob-2 and blob-3 are deleted
    assertThat(captor.firstValue.namesList).containsExactly(resourceId(2), resourceId(3))

    buffer.shutdown()
  }

  @Test
  fun `bulk live-version check groups by prefix`() {
    // Both blobs share the same prefix "path/to/" — only one listBlobs call needed
    val sc = storageClientWithLiveBlobs("path/to/blob-1", "path/to/blob-3")
    val buffer = createBuffer(batchSize = 5, storageClient = sc)

    for (i in 1..4) {
      buffer.enqueue(DeleteEvent(blobUri(i), resourceId(i)))
    }

    buffer.flush()

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchDeleteImpressionMetadata(captor.capture())
    }
    // blob-1 and blob-3 are live, only blob-2 and blob-4 should be deleted
    assertThat(captor.firstValue.namesList).containsExactly(resourceId(2), resourceId(4))

    buffer.shutdown()
  }

  @Test
  fun `batch NOT_FOUND falls back to individual deletes`() {
    wheneverBlocking { impressionMetadataServiceMock.batchDeleteImpressionMetadata(any()) }
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    val buffer = createBuffer(batchSize = 5)

    for (i in 1..4) {
      buffer.enqueue(DeleteEvent(blobUri(i), resourceId(i)))
    }

    buffer.flush()

    assertThat(buffer.pendingCount()).isEqualTo(0)

    verifyBlocking(impressionMetadataServiceMock, times(1)) { batchDeleteImpressionMetadata(any()) }

    val captor = argumentCaptor<DeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(4)) {
      deleteImpressionMetadata(captor.capture())
    }
    assertThat(captor.allValues.map { it.name })
      .containsExactly(resourceId(1), resourceId(2), resourceId(3), resourceId(4))

    buffer.shutdown()
  }

  @Test
  fun `individual delete treats NOT_FOUND as success`() {
    wheneverBlocking { impressionMetadataServiceMock.batchDeleteImpressionMetadata(any()) }
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    wheneverBlocking { impressionMetadataServiceMock.deleteImpressionMetadata(any()) }
      .thenReturn(ImpressionMetadata.getDefaultInstance())
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))
      .thenReturn(ImpressionMetadata.getDefaultInstance())
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    val buffer = createBuffer(batchSize = 5)

    for (i in 1..4) {
      buffer.enqueue(DeleteEvent(blobUri(i), resourceId(i)))
    }

    buffer.flush()

    assertThat(buffer.pendingCount()).isEqualTo(0)

    buffer.shutdown()
  }

  @Test
  fun `individual delete re-queues on non-NOT_FOUND error`() {
    wheneverBlocking { impressionMetadataServiceMock.batchDeleteImpressionMetadata(any()) }
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    wheneverBlocking { impressionMetadataServiceMock.deleteImpressionMetadata(any()) }
      .thenReturn(ImpressionMetadata.getDefaultInstance())
      .thenThrow(StatusRuntimeException(Status.UNAVAILABLE))
      .thenReturn(ImpressionMetadata.getDefaultInstance())
      .thenReturn(ImpressionMetadata.getDefaultInstance())

    val buffer = createBuffer(batchSize = 5)

    for (i in 1..4) {
      buffer.enqueue(DeleteEvent(blobUri(i), resourceId(i)))
    }

    buffer.flush()

    assertThat(buffer.pendingCount()).isEqualTo(1)

    buffer.shutdown()
  }

  @Test
  fun `non-NOT_FOUND batch error re-queues entire chunk without fallback`() {
    wheneverBlocking { impressionMetadataServiceMock.batchDeleteImpressionMetadata(any()) }
      .thenThrow(StatusRuntimeException(Status.INTERNAL))

    val buffer = createBuffer(batchSize = 5)

    for (i in 1..3) {
      buffer.enqueue(DeleteEvent(blobUri(i), resourceId(i)))
    }

    buffer.flush()

    assertThat(buffer.pendingCount()).isEqualTo(3)

    verifyBlocking(impressionMetadataServiceMock, never()) { deleteImpressionMetadata(any()) }

    buffer.shutdown()
  }
}
