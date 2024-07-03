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

package org.wfanet.panelmatch.client.exchangetasks

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.mapConcurrently
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.StorageClient.Blob
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Step.CopyOptions
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Step.CopyOptions.LabelType.BLOB
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Step.CopyOptions.LabelType.MANIFEST
import org.wfanet.panelmatch.client.storage.SigningStorageClient
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.storage.toByteString

/** Implements CopyToSharedStorageStep for regular (non-beam) tasks. */
class CopyToSharedStorageTask(
  private val source: StorageClient,
  private val destination: SigningStorageClient,
  private val copyOptions: CopyOptions,
  private val sourceBlobKey: String,
  private val destinationBlobKey: String,
  private val maxParallelTransfers: Int = 16,
) : CustomIOExchangeTask() {
  override suspend fun execute() {
    val blob = readBlob(sourceBlobKey)

    when (copyOptions.labelType) {
      BLOB -> blob.copyExternally(destinationBlobKey)
      MANIFEST -> writeManifestOutputs(blob)
      else -> error("Unrecognized CopyOptions: $copyOptions")
    }
  }

  private suspend fun writeManifestOutputs(blob: Blob) {
    val manifestBytes = blob.toByteString()
    val shardedFileName = ShardedFileName(manifestBytes.toStringUtf8())

    destination.writeBlob(destinationBlobKey, manifestBytes)

    coroutineScope {
      shardedFileName.fileNames
        .asFlow()
        .mapConcurrently(this, maxParallelTransfers) { shardName ->
          readBlob(shardName).copyExternally(shardName)
        }
        .collect()
    }
  }

  private suspend fun readBlob(blobKey: String): Blob {
    return requireNotNull(source.getBlob(blobKey)) { "Missing blob with key: $blobKey" }
  }

  private suspend fun Blob.copyExternally(destinationBlobKey: String) {
    destination.writeBlob(destinationBlobKey, read())
  }
}
