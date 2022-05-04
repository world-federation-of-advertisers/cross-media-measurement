// Copyright 2022 The Cross-Media Measurement Authors
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

import com.google.protobuf.ByteString
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollection
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step.CopyOptions
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.storage.VerifyingStorageClient
import org.wfanet.panelmatch.client.storage.VerifyingStorageClient.VerifiedBlob
import org.wfanet.panelmatch.client.storage.signatureBlobKeyFor
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.beam.breakFusion
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.storage.StorageFactory

/** Implementation of CopyFromSharedStorageStep for manifest blobs. */
fun ApacheBeamContext.copyFromSharedStorage(
  source: VerifyingStorageClient,
  destinationFactory: StorageFactory,
  copyOptions: CopyOptions,
  sourceManifestBlobKey: String,
  destinationManifestBlobKey: String,
) {
  require(copyOptions.labelType == CopyOptions.LabelType.MANIFEST) {
    "Unsupported CopyOptions: $copyOptions"
  }

  // Copy the manifest first, to avoid spinning up a Beam job if the manifest is bad.
  val shardedFileName: ShardedFileName =
    runBlocking(Dispatchers.IO) {
      val manifestBlob: VerifiedBlob = source.getBlob(sourceManifestBlobKey)
      val manifestBytes: ByteString = manifestBlob.toByteString()
      val destination: StorageClient = destinationFactory.build()
      destination.copyInternally(destinationManifestBlobKey, manifestBytes, manifestBlob.signature)
      ShardedFileName(manifestBytes.toStringUtf8())
    }

  val shardNames: PCollection<String> =
    pipeline.apply("Generate Shard Names", Create.of(shardedFileName.fileNames.asIterable()))

  shardNames.breakFusion("Break Fusion Before Copy").map("Copy Blobs From Shared Storage") {
    shardName ->
    runBlocking(Dispatchers.IO) {
      val shard: VerifiedBlob = source.getBlob(shardName)
      val destination: StorageClient = destinationFactory.build()
      destination.copyInternally(shardName, shard)
    }
    shardName
  }
}

private suspend fun StorageClient.copyInternally(
  blobKey: String,
  bytes: ByteString,
  signature: ByteString
) {
  writeBlob(signatureBlobKeyFor(blobKey), signature)
  writeBlob(blobKey, bytes)
}

private suspend fun StorageClient.copyInternally(blobKey: String, blob: VerifiedBlob) {
  writeBlob(signatureBlobKeyFor(blobKey), blob.signature)
  writeBlob(blobKey, blob.read())
}
