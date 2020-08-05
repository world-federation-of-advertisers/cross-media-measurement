// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.storage.gcs

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.withContext
import org.wfanet.measurement.storage.BYTES_PER_MIB
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.asBufferedFlow

/** Size of byte buffer used to read/write blobs from the storage system. */
private const val BYTE_BUFFER_SIZE = BYTES_PER_MIB * 1

/** Google Cloud Storage implementation of [StorageClient] for a single bucket. */
class CloudStorageClient(
  private val cloudStorage: Storage,
  private val bucketName: String
) : StorageClient<CloudStorageClient.ClientBlob> {

  override suspend fun createBlob(blobKey: String, content: Flow<ByteBuffer>): ClientBlob {
    val blob = cloudStorage.create(BlobInfo.newBuilder(bucketName, blobKey).build())

    blob.writer().use { byteChannel ->
      content.asBufferedFlow(BYTE_BUFFER_SIZE).collect { buffer ->
        withContext(Dispatchers.IO) {
          while (buffer.hasRemaining()) {
            byteChannel.write(buffer)
          }
        }
      }
    }
    return ClientBlob(blob.reload())
  }

  override fun getBlob(blobKey: String): ClientBlob? {
    val blob: Blob? = cloudStorage.get(bucketName, blobKey)
    return if (blob == null) null else ClientBlob(blob)
  }

  /** [StorageClient.Blob] implementation for [CloudStorageClient]. */
  inner class ClientBlob(private val blob: Blob) : StorageClient.Blob {
    override val size: Long
      get() = blob.size

    override fun read(flowBufferSize: Int): Flow<ByteBuffer> {
      require(flowBufferSize > 0)
      return blob.reader().asBufferedFlow(flowBufferSize)
    }

    override fun delete() {
      check(blob.delete()) { "Failed to delete blob ${blob.blobId}" }
    }
  }
}

@OptIn(ExperimentalCoroutinesApi::class) // For `onCompletion`.
private fun ReadableByteChannel.asFlow() = flow {
  var buffer = ByteBuffer.allocate(BYTE_BUFFER_SIZE)

  // Suppressed for https://youtrack.jetbrains.com/issue/IDEA-223285
  @Suppress("BlockingMethodInNonBlockingContext")
  while (read(buffer) >= 0) {
    if (buffer.position() == 0) {
      continue
    }
    buffer.flip()
    emit(buffer)
    buffer = ByteBuffer.allocate(BYTE_BUFFER_SIZE)
  }
}.onCompletion { withContext(Dispatchers.IO) { close() } }.flowOn(Dispatchers.IO)

private fun ReadableByteChannel.asBufferedFlow(flowBufferSize: Int) =
  asFlow().asBufferedFlow(flowBufferSize)
