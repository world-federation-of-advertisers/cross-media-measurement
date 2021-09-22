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
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient.VerifiedBlob
import org.wfanet.panelmatch.common.toByteString

/**
 * Executes workflows (e.g. Apache Beam pipelines) in an [ExchangeTask].
 *
 * The input blobs to [execute] are treated as manifests. In other words, each input blob is
 * expected to contain a URI specification as a UTF8 string. For example, one of these blobs might
 * contain the string "s3://some-bucket-name/some/path/data-?????-of-0128", which would imply there
 * are 128 files in some particular Amazon S3 location. This is a standard format in batch
 * processing systems like Apache Beam.
 *
 * The output of [execute] are a mixture of normal blobs and manifests.
 */
class WorkflowExchangeTask(private val outputUriPrefix: String, private val workflow: Workflow) :
  ExchangeTask {

  /** Abstraction over a workflow. */
  interface Workflow {
    data class WorkflowOutputs(
      val blobOutputs: Map<String, Flow<ByteString>>,
      val manifestOutputs: Map<String, String>
    )

    /**
     * Executes an abstract workflow.
     *
     * @param outputUriPrefix all outputs from the Workflow mentioned in the returned
     * manifestOutputs must be prefixed with this
     * @param inputUris a map from input name to a URI pattern where it can be read from
     * @return two types of outputs: inline blobs and manifests of already written data
     */
    suspend fun execute(outputUriPrefix: String, inputUris: Map<String, String>): WorkflowOutputs
  }

  override suspend fun execute(input: Map<String, VerifiedBlob>): Map<String, Flow<ByteString>> {
    val inputUris = input.mapValues { makeInput(it.value) }
    val (blobOutputs, manifestOutputs) = workflow.execute(outputUriPrefix, inputUris)
    require(blobOutputs.keys.intersect(manifestOutputs.keys).isEmpty()) {
      "WorkflowOutputs must have unique keys: ${blobOutputs.keys} and ${manifestOutputs.keys}"
    }
    return blobOutputs + manifestOutputs.mapValues { makeOutputBlob(it.value) }
  }
}

private suspend fun makeInput(blob: VerifiedBlob): String {
  return blob.read().flatten().toStringUtf8()
}

private fun makeOutputBlob(uri: String): Flow<ByteString> {
  return flowOf(uri.toByteString())
}
