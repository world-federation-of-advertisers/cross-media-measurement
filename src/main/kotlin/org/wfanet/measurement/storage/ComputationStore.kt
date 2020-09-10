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

package org.wfanet.measurement.storage

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.internal.duchy.ComputationTokenOrBuilder

private const val BLOB_KEY_PREFIX = "computations"

/**
 * Blob storage for computations.
 *
 * @param storageClient the blob storage client.
 */
class ComputationStore(private val storageClient: StorageClient) {
  /**
   * Writes a new computation blob with the specified content.
   *
   * @param token [ComputationTokenOrBuilder] from which to derive the blob key
   * @param content [Flow] producing the content to write
   * @return [Blob] with a key derived from [token]
   */
  suspend fun write(token: ComputationTokenOrBuilder, content: Flow<ByteString>): Blob {
    val blobKey = token.toBlobKey()
    val createdBlob = storageClient.createBlob(blobKey.withBlobKeyPrefix(), content)
    return Blob(blobKey, createdBlob)
  }

  /**
   * Returns a [Blob] for the computation with the specified blob key, or
   * `null` if the computation isn't found.
   */
  fun get(blobKey: String): Blob? {
    return storageClient.getBlob(blobKey.withBlobKeyPrefix())?.let {
      Blob(blobKey, it)
    }
  }

  /** [StorageClient.Blob] implementation for [ComputationStore]. */
  class Blob(val blobKey: String, wrappedBlob: StorageClient.Blob) :
    StorageClient.Blob by wrappedBlob
}

/** Returns a unique blob key derived from this [ComputationToken]. */
private fun ComputationTokenOrBuilder.toBlobKey(): String {
  return "$localComputationId/${computationStage.name}_$attempt/$blobsCount"
}

private fun String.withBlobKeyPrefix(): String {
  return "/$BLOB_KEY_PREFIX/$this"
}
