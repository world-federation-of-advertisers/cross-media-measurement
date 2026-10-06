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

package org.wfanet.measurement.securecomputation.datawatcher.testing

import com.google.protobuf.ByteString
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageClient

/** Used for in process tests to emulate google pub sub storage notifications to a [DataWatcher]. */
class DataWatcherSubscribingStorageClient(
  private val storageClient: StorageClient,
  private val storagePrefix: String,
) : StorageClient {
  private val subscribingWatchers = mutableListOf<DataWatcher>()

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    return writeBlob(blobKey, content, emptyMap())
  }

  /** Writes a blob and publishes its finalized-object metadata to each subscriber. */
  suspend fun writeBlob(
    blobKey: String,
    content: Flow<ByteString>,
    objectMetadata: Map<String, String>,
  ): StorageClient.Blob {
    val blob = storageClient.writeBlob(blobKey, content)
    publishFinalization(blob, objectMetadata)
    return blob
  }

  /**
   * Conditionally rewrites a blob and publishes only after the generation precondition succeeds.
   */
  suspend fun writeBlobIfUnchanged(
    blobKey: String,
    freshnessToken: String,
    content: Flow<ByteString>,
    objectMetadata: Map<String, String>,
  ): StorageClient.Blob {
    val conditionalStorageClient =
      storageClient as? ConditionalOperationStorageClient
        ?: error("The subscribed storage client does not support conditional writes")
    val blob = conditionalStorageClient.writeBlobIfUnchanged(blobKey, freshnessToken, content)
    publishFinalization(blob, objectMetadata)
    return blob
  }

  private suspend fun publishFinalization(
    blob: StorageClient.Blob,
    objectMetadata: Map<String, String>,
  ) {
    val finalizedObjectMetadata =
      if (blob is ConditionalOperationStorageClient.Blob) {
        objectMetadata + (DataWatcher.GENERATION_METADATA_KEY to blob.freshnessToken)
      } else {
        objectMetadata
      }

    for (dataWatcher in subscribingWatchers) {
      logger.info("Receiving path ${blob.blobKey}")
      dataWatcher.receivePath("$storagePrefix${blob.blobKey}", finalizedObjectMetadata)
    }
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    return storageClient.getBlob(blobKey)
  }

  override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> {
    return storageClient.listBlobs(prefix)
  }

  fun subscribe(watcher: DataWatcher) {
    subscribingWatchers.add(watcher)
  }

  companion object {
    internal val logger = Logger.getLogger(this::class.java.name)
  }
}
