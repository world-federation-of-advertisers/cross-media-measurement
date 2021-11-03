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
import org.wfanet.panelmatch.common.beam.keyBy
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.parDo
import org.wfanet.panelmatch.common.beam.strictOneToOneJoin
import org.wfanet.panelmatch.common.compression.CompressionParameters
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
 * @param compressionParameters specifies how to decompress the decrypted event data
 * @param serializedParameters parameters for decryption
 * @param queryResultsDecryptor decryptor
 * @param hkdfPepper pepper used in AES key derivation
 */
fun decryptQueryResults(
  encryptedQueryResults: PCollection<EncryptedQueryResult>,
  queryIdAndJoinKeys: PCollection<QueryIdAndJoinKeys>,
  compressionParameters: PCollectionView<CompressionParameters>,
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
        queryResultsDecryptor,
        serializedParameters,
        hkdfPepper,
        compressionParameters,
        privateMembershipKeys
      )
    )
}

private class DecryptQueryResults(
  private val queryResultsDecryptor: QueryResultsDecryptor,
  private val serializedParameters: ByteString,
  private val hkdfPepper: ByteString,
  private val compressionParameters: PCollectionView<CompressionParameters>,
  private val privateMembershipKeys: PCollectionView<AsymmetricKeys>
) : PTransform<PCollectionTuple, PCollection<KeyedDecryptedEventDataSet>>() {

  override fun expand(input: PCollectionTuple): PCollection<KeyedDecryptedEventDataSet> {
    val encryptedQueryResults = input[encryptedQueryResultsTag]
    val queryIdAndJoinKeys = input[queryIdAndKeysTag]

    val keyedEncryptedQueryResults: PCollection<KV<QueryId, EncryptedQueryResult>> =
      encryptedQueryResults.keyBy("Key by QueryId") { requireNotNull(it.queryId) }

    val keyedQueryIdAndKeys = queryIdAndJoinKeys.keyBy { it.queryId }
    return keyedQueryIdAndKeys
      .strictOneToOneJoin(keyedEncryptedQueryResults, name = "Join JoinKeys+QueryResults")
      .apply(
        "Make DecryptResultsFnRequests",
        ParDo.of(
            BuildDecryptQueryResultsRequestsFn(
              privateMembershipKeys,
              compressionParameters,
              serializedParameters,
              hkdfPepper
            )
          )
          .withSideInputs(privateMembershipKeys, compressionParameters)
      )
      .parDo(DecryptResultsFn(queryResultsDecryptor), name = "Decrypt")
  }

  companion object {
    val encryptedQueryResultsTag = TupleTag<EncryptedQueryResult>()
    val queryIdAndKeysTag = TupleTag<QueryIdAndJoinKeys>()
  }
}

private class BuildDecryptQueryResultsRequestsFn(
  private val keysView: PCollectionView<AsymmetricKeys>,
  private val compressionParametersView: PCollectionView<CompressionParameters>,
  private val serializedParameters: ByteString,
  private val hkdfPepper: ByteString
) :
  DoFn<
    KV<QueryIdAndJoinKeys, EncryptedQueryResult>,
    KV<QueryIdAndJoinKeys, DecryptQueryResultsRequest>>() {
  @ProcessElement
  fun processElement(context: ProcessContext) {
    val keys = context.sideInput(keysView)
    val compressionParameters = context.sideInput(compressionParametersView)

    val request = decryptQueryResultsRequest {
      serializedParameters = this@BuildDecryptQueryResultsRequestsFn.serializedParameters
      hkdfPepper = this@BuildDecryptQueryResultsRequestsFn.hkdfPepper
      serializedPublicKey = keys.serializedPublicKey
      serializedPrivateKey = keys.serializedPrivateKey
      this.compressionParameters = compressionParameters
      lookupKey = context.element().key.lookupKey
      encryptedQueryResults += context.element().value
    }

    context.output(kvOf(context.element().key, request))
  }
}

private class DecryptResultsFn(private val queryResultsDecryptor: QueryResultsDecryptor) :
  DoFn<KV<QueryIdAndJoinKeys, DecryptQueryResultsRequest>, KeyedDecryptedEventDataSet>() {
  private val metricsNamespace = "DecryptQueryResults"
  private val decryptionTimes = Metrics.distribution(metricsNamespace, "decryption-times")
  private val outputCounts = Metrics.distribution(metricsNamespace, "output-counts")

  @ProcessElement
  fun processElement(context: ProcessContext) {
    val (decryptedResults, time) =
      withTime { queryResultsDecryptor.decryptQueryResults(context.element().value) }

    decryptionTimes.update(time.toNanos())
    outputCounts.update(decryptedResults.eventDataSetsCount.toLong())

    for (eventDataSet in decryptedResults.eventDataSetsList) {
      val result = keyedDecryptedEventDataSet {
        hashedJoinKey = context.element().key.hashedJoinKey
        decryptedEventData += eventDataSet.decryptedEventDataList
      }
      context.output(result)
    }
  }
}
