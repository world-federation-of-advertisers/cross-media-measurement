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
import org.apache.beam.sdk.values.PCollection
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step.CopyOptions
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step.CopyOptions.LabelType.MANIFEST
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.StorageClient.Blob
import org.wfanet.panelmatch.client.storage.SigningStorageClient
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.beam.breakFusion
import org.wfanet.panelmatch.common.beam.flatMap
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.storage.StorageFactory

/** Implementation of CopyToSharedStorageStep for manifest blobs. */
fun ApacheBeamContext.copyToSharedStorage(
  sourceFactory: StorageFactory,
  destination: SigningStorageClient,
  copyOptions: CopyOptions,
  sourceManifestLabel: String,
  destinationManifestBlobKey: String,
) {
  require(copyOptions.labelType == MANIFEST) { "Unsupported CopyOptions: $copyOptions" }

  val manifestBytes: PCollection<ByteString> = readBlobAsPCollection(sourceManifestLabel)

  manifestBytes.map("Write Destination Manifest") { manifest ->
    runBlocking(Dispatchers.IO) { destination.writeBlob(destinationManifestBlobKey, manifest) }
    manifest
  }

  manifestBytes
    .flatMap("Generate Shard Names") { ShardedFileName(it.toStringUtf8()).fileNames.asIterable() }
    .breakFusion("Break Fusion Before Copy")
    .map("Copy Shards To Shared Storage") { shardName ->
      runBlocking(Dispatchers.IO) {
        val source: StorageClient = sourceFactory.build()
        val sourceBlob: Blob =
          requireNotNull(source.getBlob(shardName)) { "Missing blob with key $shardName" }
        destination.writeBlob(shardName, sourceBlob.read())
      }
      shardName
    }
}
