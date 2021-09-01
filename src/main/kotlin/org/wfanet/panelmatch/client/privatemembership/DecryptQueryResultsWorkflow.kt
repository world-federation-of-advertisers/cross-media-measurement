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

package org.wfanet.panelmatch.client.privatemembership

import com.google.protobuf.ByteString
import java.io.Serializable
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.common.beam.join
import org.wfanet.panelmatch.common.beam.keyBy
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.parDo

/**
 * Implements a query decryption engine in Apache Beam that decrypts a query result
 *
 * @param parameters tuning knobs for the workflow
 * @param privateMembershipCryptor implementation of lower-level oblivious query expansion and
 * result decryption
 */
class DecryptQueryResultsWorkflow(
  private val obliviousQueryParameters: Parameters,
  private val symmetricPrivateMembershipCryptor: SymmetricPrivateMembershipCryptor,
  private val hkdfPepper: ByteString,
) : Serializable {

  /** Tuning knobs for the [CreateQueriesWorkflow]. */
  data class Parameters(
    val obliviousQueryParameters: ObliviousQueryParameters,
    val publicKey: ByteString,
    val privateKey: ByteString
  ) : Serializable
  //
  /**
   * Decrypts [EncryptedQueryResult] into [DecryptedEventData]. Currently assumes that each
   * [QueryId] is unique across each [PCollection] and that each [QueryId] uniquely maps to a single
   * [JoinKey].
   */
  fun batchDecryptQueryResults(
    encryptedQueryResults: PCollection<EncryptedQueryResult>,
    queryIdToJoinKey: PCollection<KV<QueryId, JoinKey>>
  ): PCollection<DecryptedEventData> {
    return encryptedQueryResults
      .keyBy<EncryptedQueryResult, QueryId>("Key by Query Id") { requireNotNull(it.queryId) }
      .join<QueryId, EncryptedQueryResult, JoinKey, KV<JoinKey, EncryptedQueryResult>>(
        queryIdToJoinKey
      ) {
        _: QueryId,
        encryptedQueryResultsList: Iterable<EncryptedQueryResult>,
        joinKeys: Iterable<JoinKey> ->
        yield(kvOf(joinKeys.single(), encryptedQueryResultsList.single()))
      }
      .parDo(name = "Decrypt encrypted results") {
        val request = symmetricDecryptQueriesRequest {
          singleBlindedJoinkey = it.key
          this.encryptedQueryResults += it.value
          parameters = obliviousQueryParameters.obliviousQueryParameters
          publicKey = obliviousQueryParameters.publicKey
          privateKey = obliviousQueryParameters.privateKey
          this.hkdfPepper = hkdfPepper
        }
        val decryptedResults = symmetricPrivateMembershipCryptor.decryptQueryResults(request)
        yieldAll(decryptedResults.decryptedEventDataList)
      }
  }
}
