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
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient
import org.wfanet.panelmatch.common.ShardedFileName

/** Implements CopyFromSharedStorageStep and CopyToSharedStorageStep. */
class SharedStorageTask(
  private val step: ExchangeWorkflow.Step,
  private val copyOptions: ExchangeWorkflow.Step.CopyOptions,
  private val sourceStorageClient: VerifiedStorageClient,
  private val destinationStorageClient: VerifiedStorageClient
) : CustomIOExchangeTask() {
  private val inputLabel: String by lazy { step.inputLabelsMap.values.single() }
  private val outputLabel: String by lazy { step.outputLabelsMap.values.single() }

  override suspend fun execute() {
    val inputBlob = sourceStorageClient.getBlob(inputLabel).read()

    val outputs: Map<String, Flow<ByteString>> =
      when (copyOptions.labelType) {
        BLOB -> getBlobOutputs(inputBlob)
        MANIFEST -> getManifestOutputs(inputBlob)
        else -> error("Unrecognized CopyOptions label type: $step")
      }

    // TODO: if this is too slow, launch in parallel.
    for ((key, value) in outputs) {
      destinationStorageClient.createBlob(key, value)
    }
  }

  private fun getBlobOutputs(blobData: Flow<ByteString>): Map<String, Flow<ByteString>> {
    return mapOf(outputLabel to blobData)
  }

  private suspend fun getManifestOutputs(
    blobData: Flow<ByteString>
  ): Map<String, Flow<ByteString>> {
    val manifestBytes = blobData.flatten()
    val shardedFileName = ShardedFileName(manifestBytes.toStringUtf8())
    val outputs = mutableMapOf(outputLabel to flowOf(manifestBytes))

    for (shardName in shardedFileName.fileNames) {
      outputs[shardName] = sourceStorageClient.getBlob(shardName).read()
    }

    return outputs
  }
}
