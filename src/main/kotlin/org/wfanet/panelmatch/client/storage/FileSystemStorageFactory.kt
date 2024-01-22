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

import java.nio.file.Paths
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.storage.StorageFactory

class FileSystemStorageFactory(
  private val storageDetails: StorageDetails,
  private val exchangeDateKey: ExchangeDateKey,
) : StorageFactory {

  override fun build(): StorageClient {
    val directory = Paths.get(storageDetails.file.path, exchangeDateKey.path).toFile()
    val absolutePath = directory.absolutePath
    if (directory.exists()) {
      logger.fine("Directory already exists: $absolutePath")
    } else {
      check(directory.mkdirs()) { "Unable to create recursively directory: $absolutePath" }
      logger.fine("Created directory: $absolutePath")
    }
    return FileSystemStorageClient(directory)
  }

  companion object {
    private val logger by loggerFor()
  }
}
