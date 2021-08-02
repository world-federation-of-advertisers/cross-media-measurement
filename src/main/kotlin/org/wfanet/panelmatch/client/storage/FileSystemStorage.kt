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

package org.wfanet.panelmatch.client.storage

import com.google.protobuf.ByteString
import java.io.File
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.panelmatch.client.logger.loggerFor

/**
 * Reads input data from given path from the File System.
 *
 * @param baseDir String directory to read/write.
 */
class FileSystemStorage(baseDir: String) : Storage {
  private var storageClient: FileSystemStorageClient

  init {
    val baseFolder = File(baseDir)
    if (!baseFolder.exists()) {
      if (!baseFolder.parentFile.exists()) {
        baseFolder.parentFile.mkdir()
      }
      baseFolder.mkdir()
    }
    storageClient = FileSystemStorageClient(directory = baseFolder)
  }

  override suspend fun read(path: String): Flow<ByteString> {
    logger.fine("Read:${path}\n")
    val blob = storageClient.getBlob(path) ?: throw Storage.NotFoundException(path)
    return blob.read(4096)
  }

  override suspend fun write(path: String, data: Flow<ByteString>) {
    logger.fine("Write:${path}\n")
    storageClient.createBlob(path, data)
  }

  companion object {
    private val logger by loggerFor()
  }
}
