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
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Step.CopyOptions
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Step.CopyOptions.LabelType.BLOB
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Step.CopyOptions.LabelType.MANIFEST
import org.wfanet.panelmatch.client.storage.VerifyingStorageClient
import org.wfanet.panelmatch.client.storage.VerifyingStorageClient.VerifiedBlob
import org.wfanet.panelmatch.client.storage.signatureBlobKeyFor
import org.wfanet.panelmatch.common.ShardedFileName

/** Implements CopyFromSharedStorageStep. */
class CopyFromSharedStorageTask(
  private val source: VerifyingStorageClient,
  private val destination: StorageClient,
  private val copyOptions: CopyOptions,
  private val sourceBlobKey: String,
  private val destinationBlobKey: String,
  private val maxParallelTransfers: Int = 16,
) : CustomIOExchangeTask() {
  override suspend fun execute() {
    val blob = source.getBlob(sourceBlobKey)

    destination.writeBlob(signatureBlobKeyFor(destinationBlobKey), blob.signature)

    when (copyOptions.labelType) {
      BLOB -> blob.copyInternally(destinationBlobKey)
      MANIFEST -> writeManifestOutputs(blob)
      else -> error("Unrecognized CopyOptions: $copyOptions")
    }
  }

  private suspend fun writeManifestOutputs(blob: VerifyingStorageClient.VerifiedBlob) {
    val manifestBytes = blob.toByteString()
    val shardedFileName = ShardedFileName(manifestBytes.toStringUtf8())

    destination.writeBlob(destinationBlobKey, manifestBytes)

    coroutineScope {
      shardedFileName.fileNames
        .asFlow()
        .mapConcurrently(this, maxParallelTransfers) { shardName ->
          val shardBlob = source.getBlob(shardName)
          destination.writeBlob(signatureBlobKeyFor(shardName), shardBlob.signature)
          shardBlob.copyInternally(shardName)
        }
        .collect()
    }
  }

  private suspend fun VerifiedBlob.copyInternally(destinationBlobKey: String) {
    destination.writeBlob(destinationBlobKey, read())
  }
}
