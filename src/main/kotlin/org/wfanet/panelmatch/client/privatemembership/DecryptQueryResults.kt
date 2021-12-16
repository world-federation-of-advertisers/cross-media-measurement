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
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.ListCoder
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionTuple
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.values.TupleTag
import org.wfanet.panelmatch.client.exchangetasks.JoinKey
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyAndId
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyIdentifier
import org.wfanet.panelmatch.common.beam.groupByKey
import org.wfanet.panelmatch.common.beam.keyBy
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
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
 * @param plaintextJoinKeyAndId used to tie a decrypted result to its plaintext join key
 * @param decryptedJoinKeyAndId used to remove the final layer of encryption
 * @param queryIdAndIds a query id tied to its join key identifier
 * @param compressionParameters specifies how to decompress the decrypted event data
 * @param serializedParameters parameters for decryption
 * @param queryResultsDecryptor decryptor
 * @param hkdfPepper pepper used in AES key derivation
 */
fun decryptQueryResults(
  encryptedQueryResults: PCollection<EncryptedQueryResult>,
  plaintextJoinKeyAndIds: PCollection<JoinKeyAndId>,
  decryptedJoinKeyAndIds: PCollection<JoinKeyAndId>,
  queryIdAndIds: PCollection<QueryIdAndId>,
  compressionParameters: PCollectionView<CompressionParameters>,
  privateMembershipKeys: PCollectionView<AsymmetricKeys>,
  serializedParameters: ByteString,
  queryResultsDecryptor: QueryResultsDecryptor,
  hkdfPepper: ByteString
): PCollection<KeyedDecryptedEventDataSet> {
  return PCollectionTuple.of(DecryptQueryResults.encryptedQueryResultsTag, encryptedQueryResults)
    .and(DecryptQueryResults.queryIdAndIdsTag, queryIdAndIds)
    .and(DecryptQueryResults.decryptedJoinKeyAndIdsTag, decryptedJoinKeyAndIds)
    .and(DecryptQueryResults.plaintextJoinKeyAndIdsTag, plaintextJoinKeyAndIds)
    .apply(
      "Decrypt Query Results",
      DecryptQueryResults(
        serializedParameters,
        queryResultsDecryptor,
        hkdfPepper,
        compressionParameters,
        privateMembershipKeys
      )
    )
}

/**
 * A Join key identifier and its plaintext join key and decrypted join key. The decrypted join key
 * is used to decrypt the query results later.
 */
private data class JoinKeyGroup(
  val joinKeyIdentifier: JoinKeyIdentifier,
  val plaintextJoinKey: JoinKey,
  val decryptedJoinKey: JoinKey
) : Serializable

private class DecryptQueryResults(
  private val serializedParameters: ByteString,
  private val queryResultsDecryptor: QueryResultsDecryptor,
  private val hkdfPepper: ByteString,
  private val compressionParameters: PCollectionView<CompressionParameters>,
  private val privateMembershipKeys: PCollectionView<AsymmetricKeys>
) : PTransform<PCollectionTuple, PCollection<KeyedDecryptedEventDataSet>>() {

  override fun expand(input: PCollectionTuple): PCollection<KeyedDecryptedEventDataSet> {
    // TODO: This function has an unnecessary join. Instead, the two join key pCollections should
    // be joined before turning them into pCollections.
    val plaintextListCoder =
      KvCoder.of(
        ProtoCoder.of(JoinKeyIdentifier::class.java),
        ListCoder.of(ProtoCoder.of(Plaintext::class.java))
      )
    val encryptedQueryResults = input[encryptedQueryResultsTag]
    val queryIdAndIds = input[queryIdAndIdsTag]
    val decryptedJoinKeyAndIds = input[decryptedJoinKeyAndIdsTag]
    val plaintextJoinKeyAndIds = input[plaintextJoinKeyAndIdsTag]

    val keyedQueryIdAndIds = queryIdAndIds.keyBy("Key QueryIdAndIds by Id") { it.joinKeyIdentifier }
    val keyedDecryptedJoinKeyAndIds = decryptedJoinKeyAndIds.keyBy { it.joinKeyIdentifier }
    val decryptedJoinKeyKeyedByQueryId =
      keyedQueryIdAndIds.strictOneToOneJoin(keyedDecryptedJoinKeyAndIds).map {
        kvOf(it.key.queryId, it.value)
      }

    val keyedEncryptedQueryResults: PCollection<KV<QueryId, EncryptedQueryResult>> =
      encryptedQueryResults.keyBy("Key by QueryId") { requireNotNull(it.queryId) }
    val groupedEncryptedQueryResults =
      keyedEncryptedQueryResults.groupByKey("Group Encrypted Query Results")
    val individualDecryptedResults:
      PCollection<KV<JoinKeyIdentifier, List<@JvmWildcard Plaintext>>> =
      decryptedJoinKeyKeyedByQueryId
        .strictOneToOneJoin(groupedEncryptedQueryResults, name = "Join JoinKeys+QueryResults")
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
        .setCoder(plaintextListCoder)
    val groupedDecryptedResults: PCollection<KV<JoinKeyIdentifier, List<@JvmWildcard Plaintext>>> =
      individualDecryptedResults
        .groupByKey()
        .map { kvOf(it.key, it.value.flatten()) }
        .setCoder(plaintextListCoder)

    val keyedPlaintextJoinKeyAndIds = plaintextJoinKeyAndIds.keyBy { it.joinKeyIdentifier }
    return groupedDecryptedResults.strictOneToOneJoin(keyedPlaintextJoinKeyAndIds).map {
      keyedDecryptedEventDataSet {
        plaintextJoinKeyAndId = it.value
        decryptedEventData += it.key
      }
    }
  }

  companion object {
    val encryptedQueryResultsTag = TupleTag<EncryptedQueryResult>()
    val queryIdAndIdsTag = TupleTag<QueryIdAndId>()
    val decryptedJoinKeyAndIdsTag = TupleTag<JoinKeyAndId>()
    val plaintextJoinKeyAndIdsTag = TupleTag<JoinKeyAndId>()
  }
}

private class BuildDecryptQueryResultsRequestsFn(
  private val keysView: PCollectionView<AsymmetricKeys>,
  private val compressionParametersView: PCollectionView<CompressionParameters>,
  private val serializedParameters: ByteString,
  private val hkdfPepper: ByteString
) :
  DoFn<
    KV<JoinKeyAndId?, Iterable<@JvmWildcard EncryptedQueryResult>>,
    KV<JoinKeyIdentifier, DecryptQueryResultsRequest>>() {
  private val metricsNamespace = "DecryptQueryResults"
  private val noResults = Metrics.counter(metricsNamespace, "no-results")
  private val discardedResult = Metrics.counter(metricsNamespace, "skipped-queries")

  @ProcessElement
  fun processElement(context: ProcessContext) {
    val encryptedQueryResultsList = context.element().value.toList()
    val decryptedJoinKeyAndId = context.element().key
    if (encryptedQueryResultsList.isEmpty()) {
      return noResults.inc()
    } else if (decryptedJoinKeyAndId == null) {
      return discardedResult.inc()
    }
    val keys = context.sideInput(keysView)
    val compressionParameters = context.sideInput(compressionParametersView)
    for (item in encryptedQueryResultsList) {
      val request = decryptQueryResultsRequest {
        serializedParameters = this@BuildDecryptQueryResultsRequestsFn.serializedParameters
        hkdfPepper = this@BuildDecryptQueryResultsRequestsFn.hkdfPepper
        serializedPublicKey = keys.serializedPublicKey
        serializedPrivateKey = keys.serializedPrivateKey
        this.compressionParameters = compressionParameters
        decryptedJoinKey = decryptedJoinKeyAndId.joinKey
        encryptedQueryResults += item
      }
      context.output(kvOf(decryptedJoinKeyAndId.joinKeyIdentifier, request))
    }
  }
}

private class DecryptResultsFn(private val queryResultsDecryptor: QueryResultsDecryptor) :
  DoFn<
    KV<JoinKeyIdentifier, DecryptQueryResultsRequest>,
    KV<JoinKeyIdentifier, List<@JvmWildcard Plaintext>>>() {
  private val metricsNamespace = "DecryptQueryResults"
  private val decryptionTimes = Metrics.distribution(metricsNamespace, "decryption-times")
  private val outputCounts = Metrics.distribution(metricsNamespace, "output-counts")

  @ProcessElement
  fun processElement(context: ProcessContext) {
    val (decryptedResults, time) =
      withTime { queryResultsDecryptor.decryptQueryResults(context.element().value) }

    decryptionTimes.update(time.toNanos())
    outputCounts.update(decryptedResults.eventDataSetsCount.toLong())
    val key = context.element().key
    for (eventDataSet in decryptedResults.eventDataSetsList) {
      val result = kvOf(key, eventDataSet.decryptedEventDataList)
      context.output(result)
    }
  }
}
