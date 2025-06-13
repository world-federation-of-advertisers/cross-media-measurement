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
import org.wfanet.measurement.storage.StorageClient

/** Used for in process tests to emulate google pub sub storage notifications to a [DataWatcher]. */
class DataWatcherSubscribingStorageClient(
  private val storageClient: StorageClient,
  private val storagePrefix: String,
) : StorageClient {
  private var subscribingWatchers = mutableListOf<DataWatcher>()

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val blob = storageClient.writeBlob(blobKey, content)

    for (dataWatcher in subscribingWatchers) {
      logger.info("Receiving path $blobKey")
      dataWatcher.receivePath("$storagePrefix$blobKey")
    }

    return blob
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
