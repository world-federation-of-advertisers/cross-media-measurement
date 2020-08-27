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

import java.nio.ByteBuffer
import kotlinx.coroutines.flow.Flow

private const val BLOB_KEY_PREFIX = "metric-values"

/**
 * Blob storage for metric values.
 *
 * @param storageClient the blob storage client.
 * @param generateBlobKey a function to generate unique blob keys.
 */
class MetricValueStore(
  private val storageClient: StorageClient,
  private val generateBlobKey: () -> String
) {

  /**
   * Writes a metric value as a new blob with the specified content.
   *
   * @param a [Flow] producing the content to write.
   * @return the key for the new blob.
   */
  suspend fun write(content: Flow<ByteBuffer>): String {
    val blobKey = generateBlobKey()
    storageClient.createBlob(blobKey.withBlobKeyPrefix(), content)
    return blobKey
  }

  /**
   * Returns a [Blob][StorageClient.Blob] for the metric value with the
   * specified blob key, or `null` if the metric value isn't found.
   */
  fun get(blobKey: String): StorageClient.Blob? = storageClient.getBlob(blobKey.withBlobKeyPrefix())
}

private fun String.withBlobKeyPrefix(): String {
  return "/$BLOB_KEY_PREFIX/$this"
}
