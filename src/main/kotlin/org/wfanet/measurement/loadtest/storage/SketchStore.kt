// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.storage

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.storage.StorageClient

private const val BLOB_KEY_PREFIX = "sketches"

/**
 * Blob storage for sketch (for test purpose only).
 *
 * The blob path is deterministic such that the caller doesn't need to cache it.
 *
 * @param storageClient the blob storage client.
 */
class SketchStore(private val storageClient: StorageClient) {
  /** Writes a sketch as a new blob with the specified content. */
  suspend fun write(blobKey: String, content: Flow<ByteString>): Blob {
    val createdBlob = storageClient.createBlob(blobKey.withBlobKeyPrefix(), content)
    return Blob(blobKey, createdBlob)
  }

  /**
   * Returns a [Blob] for the sketch with the specified blob key, or `null` if the sketch isn't
   * found.
   */
  fun get(blobKey: String): Blob? {
    return storageClient.getBlob(blobKey.withBlobKeyPrefix())?.let { Blob(blobKey, it) }
  }

  /** [StorageClient.Blob] implementation for [SketchStore]. */
  class Blob(val blobKey: String, wrappedBlob: StorageClient.Blob) :
    StorageClient.Blob by wrappedBlob
}

private fun String.withBlobKeyPrefix(): String {
  return "/$BLOB_KEY_PREFIX/$this"
}
