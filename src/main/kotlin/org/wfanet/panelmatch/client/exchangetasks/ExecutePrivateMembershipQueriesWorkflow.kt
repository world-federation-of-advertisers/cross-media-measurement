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

import java.security.PrivateKey
import java.security.cert.X509Certificate
import org.apache.beam.sdk.Pipeline
import org.wfanet.panelmatch.client.exchangetasks.WorkflowExchangeTask.Workflow.WorkflowOutputs
import org.wfanet.panelmatch.client.privatemembership.DatabaseEntry
import org.wfanet.panelmatch.client.privatemembership.EvaluateQueriesWorkflow
import org.wfanet.panelmatch.client.privatemembership.JniQueryEvaluator
import org.wfanet.panelmatch.client.privatemembership.QueryBundle
import org.wfanet.panelmatch.client.privatemembership.QueryEvaluatorParameters
import org.wfanet.panelmatch.common.beam.SignedFiles
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map

/**
 * Runs [EvaluateQueriesWorkflow].
 *
 * TODO(@efoxepstein): this needs to be completed. For now it's just an example.
 */
class ExecutePrivateMembershipQueriesWorkflow(
  private val workflowParameters: EvaluateQueriesWorkflow.Parameters,
  private val queryEvaluatorParameters: QueryEvaluatorParameters,
  private val x509Certificate: X509Certificate,
  private val privateKey: PrivateKey
) : WorkflowExchangeTask.Workflow {
  override suspend fun execute(
    outputUriPrefix: String,
    inputUris: Map<String, String>
  ): WorkflowOutputs {
    val pipeline = Pipeline.create()
    val inputs =
      inputUris.mapValues {
        pipeline.apply(SignedFiles.read(inputUris[it.value]!!, x509Certificate))
      }

    val evaluateQueriesWorkflow =
      EvaluateQueriesWorkflow(workflowParameters, JniQueryEvaluator(queryEvaluatorParameters))

    val database =
      inputs["event-data"]!!.map {
        val databaseEntry = DatabaseEntry.parseFrom(it)
        kvOf(databaseEntry.databaseKey, databaseEntry.plaintext)
      }

    val queries = inputs["encrypted-queries"]!!.map { QueryBundle.parseFrom(it) }

    val results = evaluateQueriesWorkflow.batchEvaluateQueries(database, queries)

    // TODO(@efoxepstein): rethink the number of shards
    // Heuristic: put ~100 query results per file.
    val numShards = minOf(1, with(workflowParameters) { numShards * maxQueriesPerShard } / 100)

    val resultsSpec = "$outputUriPrefix/encrypted-results-*-of-$numShards"
    results
      .map { it.toByteString() }
      .apply(SignedFiles.write(resultsSpec, privateKey, x509Certificate))

    pipeline.run()

    return WorkflowOutputs(emptyMap(), mapOf("encrypted-results" to resultsSpec))
  }
}
