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
import kotlinx.coroutines.runBlocking
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
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class BufferedDataAvailabilityCleanupTest {

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/dataProvider123"
    private const val BLOB_URI = "gs://bucket/path/to/blob"
  }

  private fun resourceId(index: Int) =
    "$DATA_PROVIDER_NAME/impressionMetadata/im-$index"

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

  private val emptyStorageClient: FileSystemStorageClient
    get() = FileSystemStorageClient(tempFolder.newFolder("empty-storage"))

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(impressionMetadataServiceMock) }

  private fun createBuffer(
    batchSize: Int = 100,
    flushIntervalSeconds: Long = 300,
  ): BufferedDataAvailabilityCleanup {
    return BufferedDataAvailabilityCleanup(
      impressionMetadataServiceStub = impressionMetadataStub,
      dataProviderName = DATA_PROVIDER_NAME,
      storageClient = emptyStorageClient,
      batchSize = batchSize,
      flushIntervalSeconds = flushIntervalSeconds,
    )
  }

  @Test
  fun `enqueue buffers events without immediate processing`() {
    val buffer = createBuffer(batchSize = 10)

    buffer.enqueue(DeleteEvent(BLOB_URI, resourceId(1)))

    assertThat(buffer.pendingCount()).isEqualTo(1)
    verifyBlocking(impressionMetadataServiceMock, never()) {
      batchDeleteImpressionMetadata(any())
    }

    buffer.shutdown()
  }

  @Test
  fun `flush calls single batch delete RPC for multiple events with resource IDs`() {
    val buffer = createBuffer()

    buffer.enqueue(DeleteEvent("${BLOB_URI}-1", resourceId(1)))
    buffer.enqueue(DeleteEvent("${BLOB_URI}-2", resourceId(2)))
    buffer.enqueue(DeleteEvent("${BLOB_URI}-3", resourceId(3)))

    assertThat(buffer.pendingCount()).isEqualTo(3)

    buffer.flush()

    assertThat(buffer.pendingCount()).isEqualTo(0)

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchDeleteImpressionMetadata(captor.capture())
    }
    assertThat(captor.firstValue.parent).isEqualTo(DATA_PROVIDER_NAME)
    assertThat(captor.firstValue.namesList).containsExactly(
      resourceId(1), resourceId(2), resourceId(3)
    )

    verifyBlocking(impressionMetadataServiceMock, never()) {
      deleteImpressionMetadata(any())
    }

    buffer.shutdown()
  }

  @Test
  fun `flush batch-resolves blob URIs via single list RPC`() {
    val buffer = createBuffer()

    buffer.enqueue(DeleteEvent("${BLOB_URI}-1", null))
    buffer.enqueue(DeleteEvent("${BLOB_URI}-2", null))
    buffer.enqueue(DeleteEvent("${BLOB_URI}-3", null))

    buffer.flush()

    val listCaptor = argumentCaptor<ListImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      listImpressionMetadata(listCaptor.capture())
    }
    assertThat(listCaptor.firstValue.filter.blobUrisList).containsExactly(
      "${BLOB_URI}-1", "${BLOB_URI}-2", "${BLOB_URI}-3"
    )

    val deleteCaptor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchDeleteImpressionMetadata(deleteCaptor.capture())
    }
    assertThat(deleteCaptor.firstValue.namesList).hasSize(3)

    buffer.shutdown()
  }

  @Test
  fun `batch size threshold triggers immediate flush with batch RPC`() {
    val buffer = createBuffer(batchSize = 3)

    buffer.enqueue(DeleteEvent("${BLOB_URI}-1", resourceId(1)))
    buffer.enqueue(DeleteEvent("${BLOB_URI}-2", resourceId(2)))
    buffer.enqueue(DeleteEvent("${BLOB_URI}-3", resourceId(3)))

    assertThat(buffer.pendingCount()).isEqualTo(0)

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchDeleteImpressionMetadata(captor.capture())
    }
    assertThat(captor.firstValue.namesList).hasSize(3)

    buffer.shutdown()
  }

  @Test
  fun `flush chunks large batches into multiple RPCs of MAX_BATCH_DELETE_SIZE`() {
    val buffer = createBuffer(batchSize = 250)

    for (i in 1..250) {
      buffer.enqueue(DeleteEvent("${BLOB_URI}-$i", resourceId(i)))
    }

    assertThat(buffer.pendingCount()).isEqualTo(0)

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(3)) {
      batchDeleteImpressionMetadata(captor.capture())
    }

    val allValues = captor.allValues
    assertThat(allValues[0].namesList).hasSize(100)
    assertThat(allValues[1].namesList).hasSize(100)
    assertThat(allValues[2].namesList).hasSize(50)

    buffer.shutdown()
  }

  @Test
  fun `shutdown flushes remaining events via batch RPC`() {
    val buffer = createBuffer()

    buffer.enqueue(DeleteEvent("${BLOB_URI}-1", resourceId(1)))
    buffer.enqueue(DeleteEvent("${BLOB_URI}-2", resourceId(2)))

    assertThat(buffer.pendingCount()).isEqualTo(2)

    buffer.shutdown()

    assertThat(buffer.pendingCount()).isEqualTo(0)
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchDeleteImpressionMetadata(any())
    }
  }

  @Test
  fun `skips events with no matching impression metadata`() {
    wheneverBlocking { impressionMetadataServiceMock.listImpressionMetadata(any()) }
      .thenReturn(listImpressionMetadataResponse {})

    val buffer = createBuffer()

    buffer.enqueue(DeleteEvent("${BLOB_URI}-missing", null))
    buffer.enqueue(DeleteEvent("${BLOB_URI}-1", resourceId(1)))

    buffer.flush()

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchDeleteImpressionMetadata(captor.capture())
    }
    assertThat(captor.firstValue.namesList).containsExactly(resourceId(1))

    buffer.shutdown()
  }

  @Test
  fun `periodic timer flushes remaining events`() {
    val buffer = createBuffer(flushIntervalSeconds = 1)

    buffer.enqueue(DeleteEvent("${BLOB_URI}-1", resourceId(1)))

    assertThat(buffer.pendingCount()).isEqualTo(1)

    Thread.sleep(2500)

    assertThat(buffer.pendingCount()).isEqualTo(0)
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchDeleteImpressionMetadata(any())
    }

    buffer.shutdown()
  }
}
