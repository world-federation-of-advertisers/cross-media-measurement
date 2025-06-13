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
import org.wfanet.measurement.storage.StorageClient

/** Wraps [delegate] by adding [prefix] before each blob key. */
class PrefixedStorageClient(private val delegate: StorageClient, private val prefix: String) :
  StorageClient {
  init {
    require(!prefix.endsWith('/'))
  }

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    return delegate.writeBlob(applyPrefix(blobKey), content)
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    return delegate.getBlob(applyPrefix(blobKey))
  }

  override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> {
    return if (prefix.isNullOrEmpty()) {
      delegate.listBlobs("${this.prefix}/")
    } else {
      delegate.listBlobs("${this.prefix}/$prefix")
    }
  }

  private fun applyPrefix(blobKey: String): String = "$prefix/$blobKey"
}

/** Convenient function for wrapping a [StorageClient] with a [PrefixedStorageClient]. */
fun StorageClient.withPrefix(prefix: String): StorageClient {
  return PrefixedStorageClient(this, prefix)
}
