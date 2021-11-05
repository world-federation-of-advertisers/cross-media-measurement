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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step.CopyOptions.LabelType.BLOB
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step.CopyOptions.LabelType.MANIFEST
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.common.ShardedFileName

/** Implements CopyFromSharedStorageStep and CopyToSharedStorageStep. */
class SharedStorageTask(
  private val readFromSource: suspend (String) -> Flow<ByteString>,
  private val writeToDestination: suspend (String, Flow<ByteString>) -> Unit,
  private val copyOptions: ExchangeWorkflow.Step.CopyOptions,
  private val sourceBlobKey: String,
  private val destinationBlobKey: String
) : CustomIOExchangeTask() {
  override suspend fun execute() {
    val input = readFromSource(sourceBlobKey)

    val outputs: Map<String, Flow<ByteString>> =
      when (copyOptions.labelType) {
        BLOB -> getBlobOutputs(input)
        MANIFEST -> getManifestOutputs(input)
        else -> error("Unrecognized CopyOptions: $copyOptions")
      }

    // TODO: if this is too slow, launch in parallel.
    for ((key, value) in outputs) {
      writeToDestination(key, value)
    }
  }

  private fun getBlobOutputs(blobData: Flow<ByteString>): Map<String, Flow<ByteString>> {
    return mapOf(destinationBlobKey to blobData)
  }

  private suspend fun getManifestOutputs(
    blobData: Flow<ByteString>
  ): Map<String, Flow<ByteString>> {
    val manifestBytes = blobData.flatten()
    val shardedFileName = ShardedFileName(manifestBytes.toStringUtf8())
    val outputs = mutableMapOf(destinationBlobKey to flowOf(manifestBytes))

    for (shardName in shardedFileName.fileNames) {
      outputs[shardName] = readFromSource(shardName)
    }

    return outputs
  }
}
