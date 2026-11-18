// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.edpaggregator.testing

import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException as GcsStorageException
import com.google.protobuf.ByteString
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CancellationException
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

/** Rejects fixture replacements by comparing the upload response with the subsequent lookup. */
class FixtureGcsStorageClient(
  private val storage: Storage,
  private val bucket: String,
  private val delegate: ConditionalOperationStorageClient = GcsStorageClient(storage, bucket),
) : ConditionalOperationStorageClient by delegate {
  override suspend fun writeBlobIfNotFound(
    blobKey: String,
    content: Flow<ByteString>,
  ): StorageClient.Blob = write(blobKey, content, 0L)

  override suspend fun writeBlobIfUnchanged(
    blobKey: String,
    freshnessToken: String,
    content: Flow<ByteString>,
  ): StorageClient.Blob = write(blobKey, content, freshnessToken.toLong())

  private suspend fun write(
    blobKey: String,
    content: Flow<ByteString>,
    generation: Long,
  ): StorageClient.Blob =
    withContext(Dispatchers.IO) {
      try {
        val session =
          storage.blobWriteSession(
            BlobInfo.newBuilder(bucket, blobKey).build(),
            Storage.BlobWriteOption.generationMatch(generation),
          )
        session.open().use { channel ->
          content.collect { bytes ->
            for (buffer in bytes.asReadOnlyByteBufferList()) {
              while (buffer.hasRemaining()) {
                currentCoroutineContext().ensureActive()
                if (channel.write(buffer) == 0) yield()
              }
            }
          }
        }
        val uploaded = session.result.get(30, TimeUnit.SECONDS)
        val current = delegate.getBlob(blobKey) as? ConditionalOperationStorageClient.Blob
        if (current == null || current.freshnessToken != uploaded.generation.toString()) {
          throw BlobChangedException("Fixture $blobKey was replaced after its upload completed")
        }
        current
      } catch (e: Exception) {
        if (e is CancellationException || e is BlobChangedException) throw e
        val gcsError =
          generateSequence<Throwable>(e) { it.cause }
            .filterIsInstance<GcsStorageException>()
            .firstOrNull()
        if (gcsError?.code == 412)
          throw BlobChangedException("Fixture $blobKey changed during its write", e)
        throw StorageException("Error writing fixture $blobKey", e)
      }
    }
}
