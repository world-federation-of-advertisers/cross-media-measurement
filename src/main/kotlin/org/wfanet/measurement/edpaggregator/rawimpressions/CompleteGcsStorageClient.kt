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

package org.wfanet.measurement.edpaggregator.rawimpressions

import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException as GcsStorageException
import com.google.protobuf.ByteString
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.StorageException

/**
 * Conditional subpool publication that finalizes a resumable upload only after all input succeeds.
 *
 * common-jvm's GCS writer closes its channel on input failure, which can publish a partial object.
 * Closing is the GCS commit operation, so failed/cancelled uploads are left unfinalized; their
 * resumable sessions expire in GCS. Reads and freshness tokens retain the common client behavior.
 */
class CompleteGcsStorageClient(
  private val storage: Storage,
  private val bucketName: String,
  private val delegate: GcsStorageClient = GcsStorageClient(storage, bucketName),
) : ConditionalOperationStorageClient by delegate {
  override suspend fun writeBlobIfNotFound(
    blobKey: String,
    content: Flow<ByteString>,
  ): StorageClient.Blob = writeComplete(blobKey, 0L, content)

  override suspend fun writeBlobIfUnchanged(
    blob: StorageClient.Blob,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    require(blob.storageClient === delegate) { "Incompatible blob client" }
    require(blob is ConditionalOperationStorageClient.Blob) { "Blob has no freshness token" }
    return writeBlobIfUnchanged(blob.blobKey, blob.freshnessToken, content)
  }

  override suspend fun writeBlobIfUnchanged(
    blobKey: String,
    freshnessToken: String,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    val generation = freshnessToken.toLongOrNull()
    require(generation != null && generation > 0) { "Invalid GCS generation: $freshnessToken" }
    return writeComplete(blobKey, generation, content)
  }

  private suspend fun writeComplete(
    blobKey: String,
    generation: Long,
    content: Flow<ByteString>,
  ): StorageClient.Blob =
    withContext(Dispatchers.IO) {
      try {
        val channel =
          storage.writer(
            BlobInfo.newBuilder(bucketName, blobKey).build(),
            Storage.BlobWriteOption.generationMatch(generation),
          )
        content.collect { bytes ->
          for (buffer in bytes.asReadOnlyByteBufferList()) {
            while (buffer.hasRemaining()) {
              currentCoroutineContext().ensureActive()
              if (channel.write(buffer) == 0) yield()
            }
          }
        }
        currentCoroutineContext().ensureActive()
        // Do not put this in finally/use: close commits even after an input exception.
        channel.close()
        checkNotNull(delegate.getBlob(blobKey)) { "Blob $blobKey vanished after publication" }
      } catch (e: GcsStorageException) {
        if (e.code == 412) throw BlobChangedException("Generation conflict for $blobKey", e)
        throw StorageException("Error publishing subpool blob $blobKey", e)
      }
    }
}
