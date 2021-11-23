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
import com.google.protobuf.MessageLite
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.privatemembership.ReadAsSingletonPCollection
import org.wfanet.panelmatch.client.privatemembership.ReadShardedData
import org.wfanet.panelmatch.client.privatemembership.WriteShardedData
import org.wfanet.panelmatch.client.storage.StorageFactory
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.storage.toStringUtf8

/** Base class for Apache Beam-running ExchangeTasks. */
abstract class ApacheBeamTask(private val inputLabelsMap: Map<String, String>) : ExchangeTask {
  protected abstract val storageFactory: StorageFactory

  protected val pipeline: Pipeline = Pipeline.create()

  protected suspend fun <T : MessageLite> readFromManifest(
    manifest: StorageClient.Blob,
    prototype: T
  ): PCollection<T> {
    val shardedFileName = manifest.toStringUtf8()
    return pipeline.apply(
      "Read $shardedFileName",
      ReadShardedData(prototype, shardedFileName, storageFactory)
    )
  }

  protected fun readSingleBlobAsPCollection(label: String): PCollection<ByteString> {
    val blobKey = inputLabelsMap.getValue(label)
    return pipeline.apply("Read $blobKey", ReadAsSingletonPCollection(blobKey, storageFactory))
  }

  // TODO: consider also adding a helper to write non-sharded files.
  protected fun <T : MessageLite> PCollection<T>.write(shardedFileName: ShardedFileName) {
    apply("Write ${shardedFileName.spec}", WriteShardedData(shardedFileName.spec, storageFactory))
  }
}
