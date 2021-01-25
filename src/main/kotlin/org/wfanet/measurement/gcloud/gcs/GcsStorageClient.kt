// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.gcloud.gcs

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.protobuf.ByteString
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.BYTES_PER_MIB
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.asFlow
import org.wfanet.measurement.storage.StorageClient

/**
 * Size of byte buffer used to read/write blobs from Google Cloud Storage.
 *
 * The optimal size is suggested by
 * [this article](https://medium.com/@duhroach/optimal-size-of-a-cloud-storage-fetch-8c270b511016).
 */
private const val BYTE_BUFFER_SIZE = BYTES_PER_MIB * 1

/**
 * Google Cloud Storage (GCS) implementation of [StorageClient] for a single
 * bucket.
 */
class GcsStorageClient(
  private val storage: Storage,
  private val bucketName: String
) : StorageClient {

  override val defaultBufferSizeBytes: Int
    get() = BYTE_BUFFER_SIZE

  override suspend fun createBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val blob = storage.create(BlobInfo.newBuilder(bucketName, blobKey).build())

    blob.writer().use { byteChannel ->
      content.collect { bytes ->
        val buffer = bytes.asReadOnlyByteBuffer()
        withContext(Dispatchers.IO) {
          while (buffer.hasRemaining()) {
            if (byteChannel.write(buffer) == 0) {
              // Nothing was written, so we may have a non-blocking channel
              // that nothing can be written to right now. Suspend this
              // coroutine to avoid monopolizing the thread.
              delay(1L)
            }
          }
        }
      }
    }
    return ClientBlob(blob.reload())
  }

  override fun getBlob(blobKey: String): StorageClient.Blob? {
    val blob: Blob? = storage.get(bucketName, blobKey)
    return if (blob == null) null else ClientBlob(blob)
  }

  /** [StorageClient.Blob] implementation for [GcsStorageClient]. */
  private inner class ClientBlob(private val blob: Blob) : StorageClient.Blob {
    override val storageClient: StorageClient
      get() = this@GcsStorageClient

    override val size: Long
      get() = blob.size

    override fun read(bufferSizeBytes: Int): Flow<ByteString> {
      require(bufferSizeBytes > 0)

      val byteChannelFlow = blob.reader().asFlow(defaultBufferSizeBytes)
      return if (bufferSizeBytes == defaultBufferSizeBytes) {
        byteChannelFlow // Optimization. Don't need to rebuffer.
      } else {
        byteChannelFlow.asBufferedFlow(bufferSizeBytes)
      }
    }

    override fun delete() {
      check(blob.delete()) { "Failed to delete blob ${blob.blobId}" }
    }
  }

  companion object {
    /** Constructs a [GcsStorageClient] from command-line flags. */
    fun fromFlags(gcs: GcsFromFlags) = GcsStorageClient(gcs.storage, gcs.bucket)
  }
}
