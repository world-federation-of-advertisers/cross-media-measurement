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

package org.wfanet.measurement.duchy.storage

import com.google.protobuf.ByteString
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.storage.StorageClient

private typealias ComputationBlobKeyGenerator = ComputationToken.() -> String

/**
 * Blob storage for computations.
 *
 * @param storageClient the blob storage client.
 * @param generateBlobKey a function to generate a unique blob key for a given
 *     [token][ComputationToken].
 */
class ComputationStore private constructor(
  private val storageClient: StorageClient,
  private val generateBlobKey: ComputationBlobKeyGenerator
) {
  constructor(storageClient: StorageClient) : this(
    storageClient,
    ComputationToken::generateUniqueBlobKey
  )

  /**
   * Writes a new computation blob with the specified content.
   *
   * @param token [ComputationToken] from which to derive the blob key
   * @param content [Flow] producing the content to write
   * @return [Blob] with a key derived from [token]
   */
  suspend fun write(token: ComputationToken, content: Flow<ByteString>): Blob {
    val blobKey = token.generateBlobKey()
    val createdBlob = storageClient.createBlob(blobKey, content)
    return Blob(blobKey, createdBlob)
  }

  /** @see write */
  suspend fun write(token: ComputationToken, content: ByteString): Blob =
    write(token, content.asBufferedFlow(storageClient.defaultBufferSizeBytes))

  /**
   * Returns a [Blob] for the computation with the specified blob key, or
   * `null` if the computation isn't found.
   */
  fun get(blobKey: String): Blob? {
    return storageClient.getBlob(blobKey)?.let {
      Blob(blobKey, it)
    }
  }

  /** [StorageClient.Blob] implementation for [ComputationStore]. */
  class Blob(val blobKey: String, wrappedBlob: StorageClient.Blob) :
    StorageClient.Blob by wrappedBlob

  companion object {
    fun forTesting(
      storageClient: StorageClient,
      blobKeyGenerator: ComputationBlobKeyGenerator
    ): ComputationStore {
      return ComputationStore(storageClient, blobKeyGenerator)
    }
  }
}

/** Returns a unique blob key derived from this [ComputationToken]. */
private fun ComputationToken.generateUniqueBlobKey(): String {
  return listOf(
    "/computations",
    localComputationId,
    computationStage.name,
    version,
    UUID.randomUUID().toString()
  ).joinToString("/")
}
