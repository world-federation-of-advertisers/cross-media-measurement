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
import java.security.PrivateKey
import java.security.cert.X509Certificate
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.privatemembership.DatabaseEntry
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.EvaluateQueriesWorkflow
import org.wfanet.panelmatch.client.privatemembership.QueryEvaluator
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient.VerifiedBlob
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.toSingletonView
import org.wfanet.panelmatch.common.toByteString

/** Runs [EvaluateQueriesWorkflow]. */
class ExecutePrivateMembershipQueriesTask(
  override val uriPrefix: String,
  override val privateKey: PrivateKey,
  override val localCertificate: X509Certificate,
  private val workflowParameters: EvaluateQueriesWorkflow.Parameters,
  private val queryEvaluator: QueryEvaluator,
  private val partnerCertificate: X509Certificate,
  private val encryptedQueryResultFileCount: Int,
) : ApacheBeamTask() {
  override suspend fun execute(input: Map<String, VerifiedBlob>): Map<String, Flow<ByteString>> {
    val pipeline = Pipeline.create()

    val databaseManifest = input.getValue("event-data")
    val database =
      readFromManifest(databaseManifest, localCertificate).map {
        val databaseEntry = DatabaseEntry.parseFrom(it)
        kvOf(databaseEntry.databaseKey, databaseEntry.plaintext)
      }

    val queriesManifest = input.getValue("encrypted-queries")
    val queries =
      readFromManifest(queriesManifest, partnerCertificate).map {
        EncryptedQueryBundle.parseFrom(it)
      }

    val publicKeyManifest = input.getValue("private-membership-public-key")
    val privateMembershipPublicKey =
      readFromManifest(publicKeyManifest, localCertificate).toSingletonView()

    val evaluateQueriesWorkflow = EvaluateQueriesWorkflow(workflowParameters, queryEvaluator)
    val results: PCollection<EncryptedQueryResult> =
      evaluateQueriesWorkflow.batchEvaluateQueries(database, queries, privateMembershipPublicKey)

    val resultsFileName = ShardedFileName("encrypted-results", encryptedQueryResultFileCount)
    results.map { it.toByteString() }.write(resultsFileName)

    pipeline.run()

    return mapOf("encrypted-results" to flowOf(resultsFileName.spec.toByteString()))
  }
}
