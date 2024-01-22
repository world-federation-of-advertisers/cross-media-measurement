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

package org.wfanet.panelmatch.client.deploy

import kotlin.properties.Delegates
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.common.storage.withBlobSizeLimit
import picocli.CommandLine

class BlobSizeFlags {
  @set:CommandLine.Option(
    names = ["--blob-size-limit-bytes"],
    description = ["Maximum byte size allowed for any input or output blob"],
    defaultValue = (1 shl 31).toString(), // 1GiB
  )
  private var blobSizeLimitBytes by Delegates.notNull<Long>()

  fun wrapStorageFactory(
    makeStorageFactory: (StorageDetails, ExchangeDateKey) -> StorageFactory
  ): (StorageDetails, ExchangeDateKey) -> StorageFactory {
    val limit = blobSizeLimitBytes
    return { storageDetails, exchangeDateKey ->
      makeStorageFactory(storageDetails, exchangeDateKey).withBlobSizeLimit(limit)
    }
  }
}
