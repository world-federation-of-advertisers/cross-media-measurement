// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageClient

/** Reads a captured blob only if its generation matches the finalized snapshot. */
class GenerationMatchedStorageClient(
  private val delegate: ConditionalOperationStorageClient,
  private val generation: Long,
) : ConditionalOperationStorageClient by delegate {
  init {
    require(generation >= 0L) { "Blob generation must be nonnegative" }
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val blob = delegate.getBlob(blobKey) ?: return null
    if (
      generation != 0L &&
        (blob as? ConditionalOperationStorageClient.Blob)?.freshnessToken != generation.toString()
    ) {
      throw ImpressionReadException(
        blobKey,
        ImpressionReadException.Code.BLOB_GENERATION_CHANGED,
        "Blob generation differs from the finalized snapshot",
      )
    }
    return blob
  }
}
