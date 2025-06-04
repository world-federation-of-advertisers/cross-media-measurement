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

package org.wfanet.panelmatch.common.storage

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import org.wfanet.measurement.storage.StorageClient

/**
 * [StorageClient] identical to [delegate] that throws if blobs are larger than [sizeLimitBytes].
 */
class SizeLimitedStorageClient(
  private val sizeLimitBytes: Long,
  private val delegate: StorageClient,
) : StorageClient {

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    var size = 0L
    val checkedContent =
      content.onEach {
        size += it.size()
        require(size <= sizeLimitBytes) { "Exceeds maximum allowed size of $sizeLimitBytes bytes" }
      }

    return delegate.writeBlob(blobKey, checkedContent)
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val delegateBlob = delegate.getBlob(blobKey) ?: return null

    val trueSize = delegateBlob.size
    check(trueSize <= sizeLimitBytes) {
      "Blob is $trueSize bytes, which exceeds allowed size of $sizeLimitBytes bytes"
    }

    return Blob(this, delegateBlob)
  }

  override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> {
    return delegate.listBlobs(prefix).map { Blob(this, it) }
  }

  private class Blob(override val storageClient: StorageClient, delegate: StorageClient.Blob) :
    StorageClient.Blob by delegate
}
