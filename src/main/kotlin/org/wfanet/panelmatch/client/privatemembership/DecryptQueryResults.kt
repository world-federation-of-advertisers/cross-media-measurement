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
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionTuple
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.values.TupleTag
import org.wfanet.panelmatch.client.eventpostprocessing.UncompressEventsFn
import org.wfanet.panelmatch.common.beam.keyBy
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.mapWithSideInput
import org.wfanet.panelmatch.common.beam.parDo
import org.wfanet.panelmatch.common.beam.strictOneToOneJoin
import org.wfanet.panelmatch.common.compression.Compressor
import org.wfanet.panelmatch.common.crypto.AsymmetricKeys
import org.wfanet.panelmatch.common.withTime

/**
 * Decrypts and decompresses [encryptedQueryResults].
 *
 * There must be a bijection between [QueryId]s in [queryIdAndJoinKeys] and the queries present in
 * [encryptedQueryResults].
 *
 * @param encryptedQueryResults data to be decrypted and decompressed
 * @param queryIdAndJoinKeys lookup keys from which AES keys are derived and a hashed join key
 * @param compressor decompresses compressed payloads
 * @param serializedParameters parameters for decryption
 * @param queryResultsDecryptor decryptor
 * @param hkdfPepper pepper used in AES key derivation
 */
fun decryptQueryResults(
  encryptedQueryResults: PCollection<EncryptedQueryResult>,
  queryIdAndJoinKeys: PCollection<QueryIdAndJoinKeys>,
  compressor: PCollectionView<Compressor>,
  privateMembershipKeys: PCollectionView<AsymmetricKeys>,
  serializedParameters: ByteString,
  queryResultsDecryptor: QueryResultsDecryptor,
  hkdfPepper: ByteString
): PCollection<KeyedDecryptedEventDataSet> {
  return PCollectionTuple.of(DecryptQueryResults.encryptedQueryResultsTag, encryptedQueryResults)
    .and(DecryptQueryResults.queryIdAndKeysTag, queryIdAndJoinKeys)
    .apply(
      "Decrypt Query Results",
      DecryptQueryResults(
        serializedParameters,
        queryResultsDecryptor,
        hkdfPepper,
        compressor,
        privateMembershipKeys
      )
    )
}

private class DecryptQueryResults(
  private val serializedParameters: ByteString,
  private val queryResultsDecryptor: QueryResultsDecryptor,
  private val hkdfPepper: ByteString,
  private val compressor: PCollectionView<Compressor>,
  private val privateMembershipKeys: PCollectionView<AsymmetricKeys>
) : PTransform<PCollectionTuple, PCollection<KeyedDecryptedEventDataSet>>() {

  override fun expand(input: PCollectionTuple): PCollection<KeyedDecryptedEventDataSet> {
    val encryptedQueryResults = input[encryptedQueryResultsTag]
    val queryIdAndJoinKeys = input[queryIdAndKeysTag]

    val keyedEncryptedQueryResults =
      encryptedQueryResults.keyBy("Key by Query Id") { requireNotNull(it.queryId) }

    val requestTemplate = decryptQueryResultsRequest {
      serializedParameters = this@DecryptQueryResults.serializedParameters
      hkdfPepper = this@DecryptQueryResults.hkdfPepper
    }
    val keyedQueryIdAndKeys = queryIdAndJoinKeys.keyBy { it.queryId }
    val decryptedQueryResults: PCollection<DecryptedEventDataSet> =
      keyedQueryIdAndKeys
        .strictOneToOneJoin(keyedEncryptedQueryResults, name = "Join JoinKeys+QueryResults")
        .mapWithSideInput(privateMembershipKeys, name = "Make Decryption Requests") { kv, keys ->
          requestTemplate.copy {
            lookupKey = kv.key.lookupKey
            this.encryptedQueryResults += kv.value
            serializedPublicKey = keys.serializedPublicKey
            serializedPrivateKey = keys.serializedPrivateKey
          }
        }
        .parDo(DecryptResultsFn(queryResultsDecryptor), name = "Decrypt")

    return decryptedQueryResults
      .apply("Uncompress", ParDo.of(UncompressEventsFn(compressor)).withSideInputs(compressor))
      .keyBy { it.queryId }
      .strictOneToOneJoin(keyedQueryIdAndKeys, name = "Join JoinKeys+UnCompressedResults")
      .map<KV<DecryptedEventDataSet, QueryIdAndJoinKeys>, KeyedDecryptedEventDataSet> {
        keyedDecryptedEventDataSet {
          hashedJoinKey = it.value.hashedJoinKey
          decryptedEventData += it.key.decryptedEventDataList
        }
      }
  }

  companion object {
    val encryptedQueryResultsTag = TupleTag<EncryptedQueryResult>()
    val queryIdAndKeysTag = TupleTag<QueryIdAndJoinKeys>()
  }
}

private class DecryptResultsFn(private val queryResultsDecryptor: QueryResultsDecryptor) :
  DoFn<DecryptQueryResultsRequest, DecryptedEventDataSet>() {
  private val metricsNamespace = "DecryptQueryResults"
  private val decryptionTimes = Metrics.distribution(metricsNamespace, "decryption-times")
  private val outputCounts = Metrics.distribution(metricsNamespace, "output-counts")

  @ProcessElement
  fun processElement(context: ProcessContext) {
    val (decryptedResults, time) =
      withTime { queryResultsDecryptor.decryptQueryResults(context.element()) }

    decryptionTimes.update(time.toNanos())
    outputCounts.update(decryptedResults.eventDataSetsCount.toLong())

    decryptedResults.eventDataSetsList.forEach(context::output)
  }
}
