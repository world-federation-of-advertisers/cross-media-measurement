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

import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.common.paddingNonceOf
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.EvaluateQueriesParameters
import org.wfanet.panelmatch.client.privatemembership.PaddingNonce
import org.wfanet.panelmatch.client.privatemembership.QueryEvaluator
import org.wfanet.panelmatch.client.privatemembership.databaseEntry
import org.wfanet.panelmatch.client.privatemembership.encryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.evaluateQueries
import org.wfanet.panelmatch.client.privatemembership.paddingNonces
import org.wfanet.panelmatch.common.beam.combineIntoList
import org.wfanet.panelmatch.common.beam.flatMap
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.toMapView
import org.wfanet.panelmatch.common.crypto.generateSecureRandomByteString

/** Evaluates Private Membership queries. */
suspend fun ApacheBeamContext.executePrivateMembershipQueries(
  evaluateQueriesParameters: EvaluateQueriesParameters,
  queryEvaluator: QueryEvaluator,
) {
  val database = readShardedPCollection("encrypted-event-data", databaseEntry {})
  val queries = readShardedPCollection("encrypted-queries", encryptedQueryBundle {})
  val privateMembershipPublicKey = readBlobAsView("serialized-rlwe-public-key")

  val paddingNonces =
    queries.flatMap("Build Padding Nonces") { bundle ->
      bundle.queryIdsList.map { kvOf(it, generatePaddingNonce()) }
    }

  val results: PCollection<EncryptedQueryResult> =
    evaluateQueries(
      database,
      queries,
      privateMembershipPublicKey,
      paddingNonces.toMapView("Padding Nonces View"),
      evaluateQueriesParameters,
      queryEvaluator,
    )

  results.writeShardedFiles("encrypted-results")

  paddingNonces
    .map("Extract Nonces") { it.value }
    .combineIntoList("Aggregate Padding Nonces")
    .map("Build PaddingNonces proto") { paddingNonces { nonces += it } }
    .writeSingleBlob("padding-nonces")
}

private const val PADDING_NONCE_SIZE_BYTES = 16

private fun generatePaddingNonce(): PaddingNonce {
  return paddingNonceOf(generateSecureRandomByteString(PADDING_NONCE_SIZE_BYTES))
}
