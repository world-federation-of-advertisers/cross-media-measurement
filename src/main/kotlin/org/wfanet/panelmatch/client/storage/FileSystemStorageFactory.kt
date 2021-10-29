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

import com.google.type.Date
import java.io.File
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

class FileSystemStorageFactory(
  private val storageDetails: StorageDetails,
  private val recurringExchangeId: String,
  private val exchangeDate: Date
) : StorageFactory {

  override fun build(): StorageClient {
    val exchangePath =
      "recurringExchanges/$recurringExchangeId/exchanges/${exchangeDate.toLocalDate()}"
    val directory = File("${storageDetails.file.path}/$exchangePath")
    check(directory.exists() || directory.mkdirs()) {
      "Unable to create recursively directory: ${directory.absolutePath}"
    }
    return FileSystemStorageClient(directory)
  }
}
