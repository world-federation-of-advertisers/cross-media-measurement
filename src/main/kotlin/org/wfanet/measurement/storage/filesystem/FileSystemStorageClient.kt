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

package org.wfanet.measurement.storage.filesystem

import com.google.protobuf.ByteString
import java.io.File
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.asFlow
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.storage.StorageClient

private const val DEFAULT_BUFFER_SIZE_BYTES = 1024 * 4 // 4 KiB

/**
 * [StorageClient] implementation that utilizes flat files in the specified
 * directory as blobs.
 */
class FileSystemStorageClient(private val directory: File) : StorageClient {
  init {
    require(directory.isDirectory) { "$directory is not a directory" }
  }

  override val defaultBufferSizeBytes: Int
    get() = DEFAULT_BUFFER_SIZE_BYTES

  override suspend fun createBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val file = File(directory, blobKey.base64UrlEncode())
    withContext(Dispatchers.IO) {
      require(file.createNewFile()) { "$blobKey already exists" }

      file.outputStream().channel.use { byteChannel ->
        content.collect { bytes ->
          @Suppress("BlockingMethodInNonBlockingContext") // Flow context preservation.
          byteChannel.write(bytes.asReadOnlyByteBuffer())
        }
      }
    }

    return Blob(file)
  }

  override fun getBlob(blobKey: String): StorageClient.Blob? {
    val file = File(directory, blobKey.base64UrlEncode())
    return if (file.exists()) Blob(file) else null
  }

  private inner class Blob(private val file: File) : StorageClient.Blob {
    override val storageClient: StorageClient
      get() = this@FileSystemStorageClient

    override val size: Long
      get() = file.length()

    override fun read(bufferSizeBytes: Int): Flow<ByteString> =
      file.inputStream().channel.asFlow(bufferSizeBytes)

    override fun delete() {
      file.delete()
    }
  }
}

private fun String.base64UrlEncode(): String {
  return toByteArray().base64UrlEncode()
}
