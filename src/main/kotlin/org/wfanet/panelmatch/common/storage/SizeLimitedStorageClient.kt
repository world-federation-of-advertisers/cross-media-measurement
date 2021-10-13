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
import kotlinx.coroutines.flow.onEach
import org.wfanet.measurement.storage.StorageClient

/** StorageClient identical to [delegate] that throws if blobs are larger than [sizeLimitBytes]. */
class SizeLimitedStorageClient(
  private val sizeLimitBytes: Long,
  private val delegate: StorageClient
) : StorageClient {
  override val defaultBufferSizeBytes: Int
    get() = delegate.defaultBufferSizeBytes

  override suspend fun createBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    return delegate.createBlob(blobKey, content.limitSize(sizeLimitBytes))
  }

  override fun getBlob(blobKey: String): StorageClient.Blob? {
    val delegateBlob = delegate.getBlob(blobKey) ?: return null
    return Blob(this, sizeLimitBytes, delegateBlob)
  }

  private class Blob(
    override val storageClient: StorageClient,
    private val sizeLimitBytes: Long,
    private val delegate: StorageClient.Blob
  ) : StorageClient.Blob {
    override val size: Long by lazy {
      val trueSize = delegate.size
      require(trueSize <= sizeLimitBytes) {
        "Blob is $trueSize bytes, which exceeds allowed size of $sizeLimitBytes bytes"
      }
      trueSize
    }

    override fun delete() {
      delegate.delete()
    }

    override fun read(bufferSizeBytes: Int): Flow<ByteString> {
      return delegate.read(bufferSizeBytes).limitSize(sizeLimitBytes)
    }
  }
}

private fun Flow<ByteString>.limitSize(sizeLimitBytes: Long): Flow<ByteString> {
  var size = 0L
  return onEach {
    size += it.size()
    require(size <= sizeLimitBytes) { "Exceeds maximum allowed size of $sizeLimitBytes bytes" }
  }
}
