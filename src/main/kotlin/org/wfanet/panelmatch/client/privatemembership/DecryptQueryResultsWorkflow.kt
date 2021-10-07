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
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.wfanet.panelmatch.client.eventpostprocessing.uncompressEvents
import org.wfanet.panelmatch.common.beam.keyBy
import org.wfanet.panelmatch.common.beam.parDoWithSideInput
import org.wfanet.panelmatch.common.beam.strictOneToOneJoin
import org.wfanet.panelmatch.common.compression.CompressorFactory
import org.wfanet.panelmatch.common.crypto.AsymmetricKeys

/**
 * Implements a query result decryption engine in Apache Beam that decrypts a query result
 *
 * TODO: consider passing in `queryResultsDecryptor` as a parameter to `batchDecryptQueryResults`
 *
 * @param serializedParameters tuning knobs for the workflow
 * @param queryResultsDecryptor implementation of lower-level query result decryption and encrypted
 * event decryption
 * @param hkdfPepper used with joinkey to generate AES key
 * @param compressorFactory used to build Compressor
 */
class DecryptQueryResultsWorkflow(
  private val serializedParameters: ByteString,
  private val queryResultsDecryptor: QueryResultsDecryptor,
  private val hkdfPepper: ByteString,
  private val compressorFactory: CompressorFactory,
) {

  /**
   * Decrypts [EncryptedQueryResult] into [DecryptedEventDataSet]. Currently assumes that each
   * [QueryId] is unique across each [PCollection] and that each [QueryId] uniquely maps to a single
   * [JoinKey]. Uses the dictionary to decompress the data.
   */
  fun batchDecryptQueryResults(
    encryptedQueryResults: PCollection<EncryptedQueryResult>,
    queryIdToJoinKey: PCollection<KV<QueryId, JoinKey>>,
    dictionary: PCollectionView<ByteString>,
    privateMembershipKeys: PCollectionView<AsymmetricKeys>
  ): PCollection<DecryptedEventDataSet> {
    val keyedEncryptedQueryResults =
      encryptedQueryResults.keyBy("Key by Query Id") { requireNotNull(it.queryId) }

    // Local references because DecryptQueryResultsWorkflow is not serializable.
    val serializedParameters = this.serializedParameters
    val hkdfPepper = this.hkdfPepper
    val queryResultsDecryptor = this.queryResultsDecryptor

    val decryptedQueryResults: PCollection<DecryptedEventDataSet> =
      queryIdToJoinKey.strictOneToOneJoin(keyedEncryptedQueryResults).parDoWithSideInput(
          privateMembershipKeys,
          name = "Decrypt encrypted results"
        ) { kv, keys ->
        val request = decryptQueryResultsRequest {
          singleBlindedJoinkey = kv.key
          this.encryptedQueryResults += kv.value
          serializedPublicKey = keys.serializedPublicKey
          serializedPrivateKey = keys.serializedPrivateKey

          this.serializedParameters = serializedParameters
          this.hkdfPepper = hkdfPepper
        }
        val decryptedResults = queryResultsDecryptor.decryptQueryResults(request)
        yieldAll(decryptedResults.eventDataSetsList)
      }

    return uncompressEvents(decryptedQueryResults, dictionary, compressorFactory)
  }
}
