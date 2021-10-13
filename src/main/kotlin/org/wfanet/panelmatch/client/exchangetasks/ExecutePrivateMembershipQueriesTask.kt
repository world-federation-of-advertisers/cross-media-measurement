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
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.privatemembership.DatabaseEntry
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.EvaluateQueriesParameters
import org.wfanet.panelmatch.client.privatemembership.QueryEvaluator
import org.wfanet.panelmatch.client.privatemembership.evaluateQueries
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.toSingletonView
import org.wfanet.panelmatch.common.toByteString

/** Evaluates Private Membership queries. */
class ExecutePrivateMembershipQueriesTask(
  override val uriPrefix: String,
  override val privateKey: PrivateKey,
  override val localCertificate: X509Certificate,
  private val evaluateQueriesParameters: EvaluateQueriesParameters,
  private val queryEvaluator: QueryEvaluator,
  private val partnerCertificate: X509Certificate,
  private val outputs: Outputs
) : ApacheBeamTask() {

  data class Outputs(
    val encryptedQueryResultFileName: String,
    val encryptedQueryResultFileCount: Int
  )

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {
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

    val results: PCollection<EncryptedQueryResult> =
      evaluateQueries(
        database,
        queries,
        privateMembershipPublicKey,
        evaluateQueriesParameters,
        queryEvaluator
      )

    val encryptedResultsFileSpec =
      ShardedFileName(outputs.encryptedQueryResultFileName, outputs.encryptedQueryResultFileCount)
    require(encryptedResultsFileSpec.shardCount == outputs.encryptedQueryResultFileCount)
    results.map { it.toByteString() }.write(encryptedResultsFileSpec)

    pipeline.run()

    return mapOf("encrypted-results" to flowOf(encryptedResultsFileSpec.spec.toByteString()))
  }
}
