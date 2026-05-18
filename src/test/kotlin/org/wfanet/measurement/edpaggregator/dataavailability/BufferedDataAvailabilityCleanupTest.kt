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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.DeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class BufferedDataAvailabilityCleanupTest {

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/dataProvider123"
    private const val RESOURCE_ID = "$DATA_PROVIDER_NAME/impressionMetadata/im-123"
    private const val BLOB_URI = "gs://bucket/path/to/blob"
  }

  private val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
          listImpressionMetadataResponse {
            impressionMetadata +=
              listOf(
                impressionMetadata {
                  name = RESOURCE_ID
                  modelLine = "modelLine1"
                  blobUri = request.filter.blobUriPrefix
                  interval = interval {
                    startTime = timestamp { seconds = 100 }
                    endTime = timestamp { seconds = 200 }
                  }
                  state = ImpressionMetadata.State.ACTIVE
                }
              )
          }
        }
      onBlocking { deleteImpressionMetadata(any<DeleteImpressionMetadataRequest>()) }
        .thenAnswer { ImpressionMetadata.getDefaultInstance() }
    }

  private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub by lazy {
    ImpressionMetadataServiceCoroutineStub(grpcTestServerRule.channel)
  }

  @get:Rule val tempFolder = TemporaryFolder()

  private val emptyStorageClient: FileSystemStorageClient
    get() = FileSystemStorageClient(tempFolder.newFolder("empty-storage"))

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(impressionMetadataServiceMock) }

  private fun createCleanup(): DataAvailabilityCleanup {
    return DataAvailabilityCleanup(impressionMetadataStub, DATA_PROVIDER_NAME, emptyStorageClient)
  }

  @Test
  fun `enqueue buffers events without immediate processing`() {
    val buffer =
      BufferedDataAvailabilityCleanup(
        cleanupDelegate = createCleanup(),
        batchSize = 10,
        flushIntervalSeconds = 300,
      )

    buffer.enqueue(DeleteEvent(BLOB_URI, RESOURCE_ID))

    assertThat(buffer.pendingCount()).isEqualTo(1)

    verifyBlocking(impressionMetadataServiceMock, times(0)) { deleteImpressionMetadata(any()) }

    buffer.shutdown()
  }

  @Test
  fun `flush processes all buffered events`() {
    val buffer =
      BufferedDataAvailabilityCleanup(
        cleanupDelegate = createCleanup(),
        batchSize = 100,
        flushIntervalSeconds = 300,
      )

    buffer.enqueue(DeleteEvent("${BLOB_URI}/1", RESOURCE_ID))
    buffer.enqueue(DeleteEvent("${BLOB_URI}/2", RESOURCE_ID))
    buffer.enqueue(DeleteEvent("${BLOB_URI}/3", RESOURCE_ID))

    assertThat(buffer.pendingCount()).isEqualTo(3)

    buffer.flush()

    assertThat(buffer.pendingCount()).isEqualTo(0)
    verifyBlocking(impressionMetadataServiceMock, times(3)) { deleteImpressionMetadata(any()) }

    buffer.shutdown()
  }

  @Test
  fun `batch size threshold triggers immediate flush`() {
    val buffer =
      BufferedDataAvailabilityCleanup(
        cleanupDelegate = createCleanup(),
        batchSize = 3,
        flushIntervalSeconds = 300,
      )

    buffer.enqueue(DeleteEvent("${BLOB_URI}/1", RESOURCE_ID))
    buffer.enqueue(DeleteEvent("${BLOB_URI}/2", RESOURCE_ID))

    // Not yet at threshold
    assertThat(buffer.pendingCount()).isEqualTo(2)

    // This triggers the threshold
    buffer.enqueue(DeleteEvent("${BLOB_URI}/3", RESOURCE_ID))

    // After flush, queue should be empty
    assertThat(buffer.pendingCount()).isEqualTo(0)
    verifyBlocking(impressionMetadataServiceMock, times(3)) { deleteImpressionMetadata(any()) }

    buffer.shutdown()
  }

  @Test
  fun `failed events are re-queued`() {
    // First call succeeds, second fails, third succeeds
    wheneverBlocking { impressionMetadataServiceMock.deleteImpressionMetadata(any()) }
      .thenAnswer { ImpressionMetadata.getDefaultInstance() }
      .thenAnswer { throw StatusException(Status.UNAVAILABLE) }
      .thenAnswer { ImpressionMetadata.getDefaultInstance() }

    val buffer =
      BufferedDataAvailabilityCleanup(
        cleanupDelegate = createCleanup(),
        batchSize = 100,
        flushIntervalSeconds = 300,
      )

    buffer.enqueue(DeleteEvent("${BLOB_URI}/1", RESOURCE_ID))
    buffer.enqueue(DeleteEvent("${BLOB_URI}/2", RESOURCE_ID))
    buffer.enqueue(DeleteEvent("${BLOB_URI}/3", RESOURCE_ID))

    buffer.flush()

    // One event failed and was re-queued
    assertThat(buffer.pendingCount()).isEqualTo(1)

    buffer.shutdown()
  }

  @Test
  fun `shutdown flushes remaining events`() {
    val buffer =
      BufferedDataAvailabilityCleanup(
        cleanupDelegate = createCleanup(),
        batchSize = 100,
        flushIntervalSeconds = 300,
      )

    buffer.enqueue(DeleteEvent("${BLOB_URI}/1", RESOURCE_ID))
    buffer.enqueue(DeleteEvent("${BLOB_URI}/2", RESOURCE_ID))

    assertThat(buffer.pendingCount()).isEqualTo(2)

    buffer.shutdown()

    assertThat(buffer.pendingCount()).isEqualTo(0)
    verifyBlocking(impressionMetadataServiceMock, times(2)) { deleteImpressionMetadata(any()) }
  }

  @Test
  fun `periodic flush processes buffered events`() {
    val buffer =
      BufferedDataAvailabilityCleanup(
        cleanupDelegate = createCleanup(),
        batchSize = 100,
        flushIntervalSeconds = 1,
      )

    buffer.enqueue(DeleteEvent(BLOB_URI, RESOURCE_ID))

    assertThat(buffer.pendingCount()).isEqualTo(1)

    // Wait for the periodic flush to fire
    Thread.sleep(2500)

    assertThat(buffer.pendingCount()).isEqualTo(0)
    verifyBlocking(impressionMetadataServiceMock, times(1)) { deleteImpressionMetadata(any()) }

    buffer.shutdown()
  }
}
