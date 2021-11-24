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

import com.google.protobuf.ByteString
import com.google.protobuf.Message
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.wfanet.measurement.storage.StorageClient.Blob
import org.wfanet.panelmatch.client.privatemembership.ReadAsSingletonPCollection
import org.wfanet.panelmatch.client.privatemembership.ReadShardedData
import org.wfanet.panelmatch.client.privatemembership.WriteShardedData
import org.wfanet.panelmatch.client.storage.StorageFactory
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.beam.toSingletonView
import org.wfanet.panelmatch.common.storage.toByteString
import org.wfanet.panelmatch.common.storage.toStringUtf8

class ApacheBeamContext(
  private val pipeline: Pipeline,
  private val outputManifests: Map<String, ShardedFileName>,
  private val inputLabels: Map<String, String>,
  private val inputBlobs: Map<String, Blob>,
  private val storageFactory: StorageFactory
) {
  suspend fun <T : Message> readShardedPCollection(
    manifestLabel: String,
    prototype: T
  ): PCollection<T> {
    val blob =
      requireNotNull(inputBlobs.getValue(manifestLabel)) {
        "Missing manifest with label $manifestLabel"
      }
    val shardedFileName = blob.toStringUtf8()
    return pipeline.apply(
      "Read $shardedFileName",
      ReadShardedData(prototype, shardedFileName, storageFactory)
    )
  }

  fun readBlobAsPCollection(label: String): PCollection<ByteString> {
    val blobKey = inputLabels.getValue(label)
    return pipeline.apply("Read $blobKey", ReadAsSingletonPCollection(blobKey, storageFactory))
  }

  fun readBlobAsView(label: String): PCollectionView<ByteString> {
    return readBlobAsPCollection(label).toSingletonView("View of $label")
  }

  suspend fun readBlob(label: String): ByteString {
    return inputBlobs.getValue(label).toByteString()
  }

  fun <T : Message> PCollection<T>.write(manifestLabel: String) {
    val shardedFileName = outputManifests.getValue(manifestLabel)
    apply("Write ${shardedFileName.spec}", WriteShardedData(shardedFileName.spec, storageFactory))
  }
}
