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
import io.grpc.StatusRuntimeException
import java.io.File
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.atLeast
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

  private fun emptyStorageClient(): FileSystemStorageClient =
    FileSystemStorageClient(tempFolder.newFolder("empty-${System.nanoTime()}"))

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

  @Before
  fun setUp() {
    Mockito.clearInvocations(impressionMetadataServiceMock)
  }

  private fun createBuffer(storageClient: StorageClient? = null): BufferedDataAvailabilityCleanup {
    return BufferedDataAvailabilityCleanup(
      impressionMetadataServiceStub = impressionMetadataStub,
      dataProviderName = DATA_PROVIDER_NAME,
      storageClient = storageClient ?: emptyStorageClient(),
    )
  }

  @Test
  fun `enqueue flushes synchronously`() {
    val buffer = createBuffer()

    buffer.enqueue(DeleteEvent(blobUri(1), resourceId(1)))

    assertThat(buffer.pendingCount()).isEqualTo(0)

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchDeleteImpressionMetadata(captor.capture())
    }
    assertThat(captor.firstValue.namesList).containsExactly(resourceId(1))
  }

  @Test
  fun `enqueue with pre-resolved resource ID uses batch delete`() {
    val buffer = createBuffer()

    buffer.enqueue(DeleteEvent(blobUri(1), resourceId(1)))
    buffer.enqueue(DeleteEvent(blobUri(2), resourceId(2)))
    buffer.enqueue(DeleteEvent(blobUri(3), resourceId(3)))

    assertThat(buffer.pendingCount()).isEqualTo(0)

    verifyBlocking(impressionMetadataServiceMock, never()) { deleteImpressionMetadata(any()) }
  }

  @Test
  fun `flush batch-resolves blob URIs via single list RPC`() {
    val buffer = createBuffer()

    buffer.enqueue(DeleteEvent(blobUri(1), null))

    assertThat(buffer.pendingCount()).isEqualTo(0)

    val listCaptor = argumentCaptor<ListImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      listImpressionMetadata(listCaptor.capture())
    }
    assertThat(listCaptor.firstValue.filter.blobUrisList).containsExactly(blobUri(1))

    val deleteCaptor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchDeleteImpressionMetadata(deleteCaptor.capture())
    }
    assertThat(deleteCaptor.firstValue.namesList).containsExactly(resourceId(1))
  }

  @Test
  fun `concurrent enqueues coalesce via flush lock`() {
    val buffer = createBuffer()
    val threadCount = 12
    val latch = CountDownLatch(threadCount)
    val executor = Executors.newFixedThreadPool(threadCount)

    for (i in 1..threadCount) {
      executor.submit {
        latch.countDown()
        latch.await()
        buffer.enqueue(DeleteEvent(blobUri(i), resourceId(i)))
      }
    }

    executor.shutdown()
    executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)

    assertThat(buffer.pendingCount()).isEqualTo(0)
  }

  @Test
  fun `flush chunks large batches into multiple RPCs of MAX_BATCH_DELETE_SIZE`() {
    val buffer = createBuffer()
    val count = 260
    val threadCount = 20
    val latch = CountDownLatch(threadCount)
    val executor = Executors.newFixedThreadPool(threadCount)
    val eventsPerThread = count / threadCount

    for (t in 0 until threadCount) {
      executor.submit {
        latch.countDown()
        latch.await()
        val start = t * eventsPerThread + 1
        for (i in start until start + eventsPerThread) {
          buffer.enqueue(DeleteEvent(blobUri(i), resourceId(i)))
        }
      }
    }

    executor.shutdown()
    executor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)

    assertThat(buffer.pendingCount()).isEqualTo(0)

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, atLeast(1)) {
      batchDeleteImpressionMetadata(captor.capture())
    }

    val totalDeleted = captor.allValues.sumOf { it.namesList.size }
    assertThat(totalDeleted).isEqualTo(count)

    for (request in captor.allValues) {
      assertThat(request.namesList.size).isAtMost(100)
    }
  }

  @Test
  fun `shutdown flushes remaining events`() {
    val buffer = createBuffer()

    buffer.shutdown()

    assertThat(buffer.pendingCount()).isEqualTo(0)
  }

  @Test
  fun `skips events with no matching impression metadata`() {
    wheneverBlocking { impressionMetadataServiceMock.listImpressionMetadata(any()) }
      .thenReturn(listImpressionMetadataResponse {})

    val buffer = createBuffer()

    buffer.enqueue(DeleteEvent(blobUri(9999), null))

    assertThat(buffer.pendingCount()).isEqualTo(0)

    verifyBlocking(impressionMetadataServiceMock, never()) { batchDeleteImpressionMetadata(any()) }
  }

  @Test
  fun `skips events whose blobs still have a live version`() {
    val sc = storageClientWithLiveBlobs("path/to/blob-1")
    val buffer = createBuffer(storageClient = sc)

    buffer.enqueue(DeleteEvent(blobUri(1), resourceId(1)))
    buffer.enqueue(DeleteEvent(blobUri(2), resourceId(2)))

    assertThat(buffer.pendingCount()).isEqualTo(0)

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchDeleteImpressionMetadata(captor.capture())
    }
    assertThat(captor.firstValue.namesList).containsExactly(resourceId(2))
  }

  @Test
  fun `bulk live-version check groups by prefix`() {
    val sc = storageClientWithLiveBlobs("path/to/blob-1", "path/to/blob-3")
    val buffer = createBuffer(storageClient = sc)

    buffer.enqueue(DeleteEvent(blobUri(1), resourceId(1)))
    buffer.enqueue(DeleteEvent(blobUri(2), resourceId(2)))
    buffer.enqueue(DeleteEvent(blobUri(3), resourceId(3)))

    assertThat(buffer.pendingCount()).isEqualTo(0)

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock) {
      batchDeleteImpressionMetadata(captor.capture())
    }

    val allDeleted = captor.allValues.flatMap { it.namesList }
    assertThat(allDeleted).containsExactly(resourceId(2))
  }

  @Test
  fun `batch NOT_FOUND falls back to individual deletes`() {
    wheneverBlocking { impressionMetadataServiceMock.batchDeleteImpressionMetadata(any()) }
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    val buffer = createBuffer()

    buffer.enqueue(DeleteEvent(blobUri(1), resourceId(1)))

    assertThat(buffer.pendingCount()).isEqualTo(0)

    verifyBlocking(impressionMetadataServiceMock, times(1)) { batchDeleteImpressionMetadata(any()) }

    val captor = argumentCaptor<DeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      deleteImpressionMetadata(captor.capture())
    }
    assertThat(captor.firstValue.name).isEqualTo(resourceId(1))
  }

  @Test
  fun `individual delete treats NOT_FOUND as success`() {
    wheneverBlocking { impressionMetadataServiceMock.batchDeleteImpressionMetadata(any()) }
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    wheneverBlocking { impressionMetadataServiceMock.deleteImpressionMetadata(any()) }
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    val buffer = createBuffer()

    buffer.enqueue(DeleteEvent(blobUri(1), resourceId(1)))

    assertThat(buffer.pendingCount()).isEqualTo(0)
  }

  @Test
  fun `individual delete re-queues on non-NOT_FOUND error`() {
    wheneverBlocking { impressionMetadataServiceMock.batchDeleteImpressionMetadata(any()) }
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    wheneverBlocking { impressionMetadataServiceMock.deleteImpressionMetadata(any()) }
      .thenThrow(StatusRuntimeException(Status.UNAVAILABLE))

    val buffer = createBuffer()

    buffer.enqueue(DeleteEvent(blobUri(1), resourceId(1)))

    assertThat(buffer.pendingCount()).isEqualTo(1)
  }

  @Test
  fun `non-NOT_FOUND batch error re-queues with original blob path`() {
    wheneverBlocking { impressionMetadataServiceMock.batchDeleteImpressionMetadata(any()) }
      .thenThrow(StatusRuntimeException(Status.INTERNAL))

    val buffer = createBuffer()

    buffer.enqueue(DeleteEvent(blobUri(1), resourceId(1)))

    assertThat(buffer.pendingCount()).isEqualTo(1)

    verifyBlocking(impressionMetadataServiceMock, never()) { deleteImpressionMetadata(any()) }
  }
}
