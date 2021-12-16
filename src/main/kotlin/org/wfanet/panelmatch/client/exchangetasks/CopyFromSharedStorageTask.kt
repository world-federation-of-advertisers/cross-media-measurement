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

import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step.CopyOptions
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step.CopyOptions.LabelType.BLOB
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step.CopyOptions.LabelType.MANIFEST
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.createBlob
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient.Companion.signatureBlobKeyFor
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient.VerifiedBlob
import org.wfanet.panelmatch.common.ShardedFileName

/** Implements CopyFromSharedStorageStep. */
class CopyFromSharedStorageTask(
  private val source: VerifiedStorageClient,
  private val destination: StorageClient,
  private val copyOptions: CopyOptions,
  private val sourceBlobKey: String,
  private val destinationBlobKey: String
) : CustomIOExchangeTask() {
  override suspend fun execute() {
    val blob = source.getBlob(sourceBlobKey)

    destination.createBlob(signatureBlobKeyFor(destinationBlobKey), blob.signature)

    when (copyOptions.labelType) {
      BLOB -> blob.copyInternally(destinationBlobKey)
      MANIFEST -> writeManifestOutputs(blob)
      else -> error("Unrecognized CopyOptions: $copyOptions")
    }
  }

  private suspend fun writeManifestOutputs(blob: VerifiedBlob) {
    val manifestBytes = blob.toByteString()
    val shardedFileName = ShardedFileName(manifestBytes.toStringUtf8())

    destination.createBlob(destinationBlobKey, manifestBytes)

    for (shardName in shardedFileName.fileNames) {
      val shardBlob = source.getBlob(shardName)
      destination.createBlob(signatureBlobKeyFor(shardName), shardBlob.signature)
      shardBlob.copyInternally(shardName)
    }
  }

  private suspend fun VerifiedBlob.copyInternally(destinationBlobKey: String) {
    destination.createBlob(destinationBlobKey, read())
  }
}
