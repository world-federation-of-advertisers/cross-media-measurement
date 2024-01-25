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
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.PipelineResult
import org.wfanet.measurement.storage.StorageClient.Blob
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.storage.StorageFactory

/** Executes Exchange Steps that rely on Apache Beam. */
class ApacheBeamTask(
  private val pipeline: Pipeline,
  private val storageFactory: StorageFactory,
  private val inputLabels: Map<String, String>,
  private val outputLabels: Map<String, String>,
  private val outputManifests: Map<String, ShardedFileName>,
  private val skipReadInput: Boolean,
  private val executeOnPipeline: suspend ApacheBeamContext.() -> Unit,
) : ExchangeTask {
  override suspend fun execute(input: Map<String, Blob>): Map<String, Flow<ByteString>> {
    val context =
      ApacheBeamContext(pipeline, outputLabels, inputLabels, outputManifests, input, storageFactory)
    context.executeOnPipeline()

    try {
      val finalState = pipeline.run().waitUntilFinish()
      check(finalState == PipelineResult.State.DONE) { "Pipeline is in state $finalState" }
    } catch (e: Exception) {
      throw ExchangeTaskFailedException.ofPermanent(e)
    }

    return outputManifests.mapValues { flowOf(it.value.spec.toByteStringUtf8()) }
  }

  override fun skipReadInput(): Boolean = skipReadInput
}
