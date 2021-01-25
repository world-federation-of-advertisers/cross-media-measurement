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
import org.wfanet.measurement.storage.StorageClient

private const val BLOB_KEY_PREFIX = "metric-values"

private typealias BlobKeyGenerator = () -> String

/**
 * Blob storage for metric values.
 *
 * @param storageClient the blob storage client.
 * @param generateBlobKey a function to generate unique blob keys.
 */
class MetricValueStore private constructor(
  private val storageClient: StorageClient,
  private val generateBlobKey: BlobKeyGenerator
) {
  constructor(storageClient: StorageClient) : this(storageClient, { UUID.randomUUID().toString() })

  /**
   * Writes a metric value as a new blob with the specified content.
   *
   * @param a [Flow] producing the content to write.
   * @return a [Blob] with a generated blob key.
   */
  suspend fun write(content: Flow<ByteString>): Blob {
    val blobKey = generateBlobKey()
    val createdBlob = storageClient.createBlob(blobKey.withBlobKeyPrefix(), content)
    return Blob(blobKey, createdBlob)
  }

  /**
   * Returns a [Blob] for the metric value with the specified blob key, or
   * `null` if the metric value isn't found.
   */
  fun get(blobKey: String): Blob? {
    return storageClient.getBlob(blobKey.withBlobKeyPrefix())?.let {
      Blob(blobKey, it)
    }
  }

  /** [StorageClient.Blob] implementation for [MetricValueStore]. */
  class Blob(val blobKey: String, wrappedBlob: StorageClient.Blob) :
    StorageClient.Blob by wrappedBlob

  companion object {
    fun forTesting(storageClient: StorageClient, generateBlobKey: BlobKeyGenerator) =
      MetricValueStore(storageClient, generateBlobKey)
  }
}

private fun String.withBlobKeyPrefix(): String {
  return "/$BLOB_KEY_PREFIX/$this"
}
