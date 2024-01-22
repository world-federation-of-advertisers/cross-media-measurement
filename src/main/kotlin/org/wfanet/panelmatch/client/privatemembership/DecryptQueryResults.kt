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

import com.google.protobuf.Any
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
import org.wfanet.panelmatch.client.common.joinKeyOf
import org.wfanet.panelmatch.client.exchangetasks.JoinKey
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyAndId
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyIdentifier
import org.wfanet.panelmatch.client.exchangetasks.joinKeyAndId
import org.wfanet.panelmatch.common.beam.filter
import org.wfanet.panelmatch.common.beam.flatten
import org.wfanet.panelmatch.common.beam.groupByKey
import org.wfanet.panelmatch.common.beam.keyBy
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.oneToOneJoin
import org.wfanet.panelmatch.common.beam.parDo
import org.wfanet.panelmatch.common.beam.strictOneToOneJoin
import org.wfanet.panelmatch.common.beam.toPCollectionList
import org.wfanet.panelmatch.common.compression.CompressionParameters
import org.wfanet.panelmatch.common.crypto.AsymmetricKeyPair
import org.wfanet.panelmatch.common.withTime

/**
 * Decrypted and Plaintext JoinKeyAndId inputs should have discardedJoinKeys filtered out before
 * decryption.
 */
fun List<JoinKeyAndId>.removeDiscardedJoinKeys(
  discardedJoinKeys: List<JoinKeyIdentifier>
): List<JoinKeyAndId> {
  val discardedJoinKeySet = discardedJoinKeys.toSet()
  return filter { !discardedJoinKeySet.contains(it.joinKeyIdentifier) }
}

/**
 * Decrypts and decompresses [encryptedQueryResults].
 *
 * @param encryptedQueryResults data to be decrypted and decompressed
 * @param plaintextJoinKeyAndIds used to tie a decrypted result to its plaintext join key
 * @param decryptedJoinKeyAndIds used to remove the final layer of encryption
 * @param queryIdAndIds a query id tied to its join key identifier
 * @param compressionParameters specifies how to decompress the decrypted event data
 * @param parameters parameters for decryption
 * @param queryResultsDecryptor decryptor
 * @param hkdfPepper pepper used in AES key derivation
 */
fun decryptQueryResults(
  encryptedQueryResults: PCollection<EncryptedQueryResult>,
  plaintextJoinKeyAndIds: PCollection<JoinKeyAndId>,
  decryptedJoinKeyAndIds: PCollection<JoinKeyAndId>,
  queryIdAndIds: PCollection<QueryIdAndId>,
  compressionParameters: PCollectionView<CompressionParameters>,
  privateMembershipKeys: PCollectionView<AsymmetricKeyPair>,
  parameters: Any,
  queryResultsDecryptor: QueryResultsDecryptor,
  hkdfPepper: ByteString,
): PCollection<KeyedDecryptedEventDataSet> {
  return PCollectionTuple.of(DecryptQueryResults.encryptedQueryResultsTag, encryptedQueryResults)
    .and(DecryptQueryResults.queryIdAndIdsTag, queryIdAndIds)
    .and(DecryptQueryResults.decryptedJoinKeyAndIdsTag, decryptedJoinKeyAndIds)
    .and(DecryptQueryResults.plaintextJoinKeyAndIdsTag, plaintextJoinKeyAndIds)
    .apply(
      "Decrypt Query Results",
      DecryptQueryResults(
        parameters,
        queryResultsDecryptor,
        hkdfPepper,
        compressionParameters,
        privateMembershipKeys,
      ),
    )
}

class DecryptQueryResults(
  private val parameters: Any,
  private val queryResultsDecryptor: QueryResultsDecryptor,
  private val hkdfPepper: ByteString,
  private val compressionParameters: PCollectionView<CompressionParameters>,
  private val privateMembershipKeys: PCollectionView<AsymmetricKeyPair>,
) : PTransform<PCollectionTuple, PCollection<KeyedDecryptedEventDataSet>>() {

  override fun expand(input: PCollectionTuple): PCollection<KeyedDecryptedEventDataSet> {
    // TODO: This function has an unnecessary join. Instead, the two join key pCollections should
    // be joined before turning them into pCollections.
    val plaintextListCoder =
      KvCoder.of(
        ProtoCoder.of(JoinKeyIdentifier::class.java),
        ListCoder.of(ProtoCoder.of(Plaintext::class.java)),
      )
    val encryptedQueryResults: PCollection<EncryptedQueryResult> = input[encryptedQueryResultsTag]
    val queryIdAndIds: PCollection<QueryIdAndId> = input[queryIdAndIdsTag]
    val decryptedJoinKeyAndIds: PCollection<JoinKeyAndId> = input[decryptedJoinKeyAndIdsTag]
    val plaintextJoinKeyAndIds: PCollection<JoinKeyAndId> = input[plaintextJoinKeyAndIdsTag]

    val keyedQueryIdAndIds: PCollection<KV<JoinKeyIdentifier, QueryIdAndId>> =
      queryIdAndIds.keyBy("Key QueryIds by Id") { it.joinKeyIdentifier }

    val keyedDecryptedJoinKeyAndIds: PCollection<KV<JoinKeyIdentifier, JoinKeyAndId>> =
      decryptedJoinKeyAndIds.keyBy("Key DecryptedJoinKeys by JoinKeyIdentifier") {
        it.joinKeyIdentifier
      }

    val decryptedJoinKeyKeyedByQueryId: PCollection<KV<QueryId, JoinKeyAndId>> =
      keyedQueryIdAndIds
        .strictOneToOneJoin(keyedDecryptedJoinKeyAndIds, name = "Join QueryIds+DecryptedJoinKeys")
        .map("Map to QueryId and JoinKey") { kvOf(it.key.queryId, it.value) }

    val keyedEncryptedQueryResults: PCollection<KV<QueryId, EncryptedQueryResult>> =
      encryptedQueryResults.keyBy("Key EncryptedQueryResult by QueryId") {
        requireNotNull(it.queryId)
      }

    val groupedEncryptedQueryResults: PCollection<KV<QueryId, Iterable<EncryptedQueryResult>>> =
      keyedEncryptedQueryResults.groupByKey("Group Encrypted Query Results")

    val individualDecryptedResults:
      PCollection<KV<JoinKeyIdentifier, List<@JvmWildcard Plaintext>>> =
      decryptedJoinKeyKeyedByQueryId
        .oneToOneJoin(groupedEncryptedQueryResults, name = "Join JoinKeys+QueryResults")
        .apply(
          "Make DecryptResultsFnRequests",
          ParDo.of(
              BuildDecryptQueryResultsParametersFn(
                privateMembershipKeys,
                compressionParameters,
                parameters,
                hkdfPepper,
              )
            )
            .withSideInputs(privateMembershipKeys, compressionParameters),
        )
        .parDo(DecryptResultsFn(queryResultsDecryptor), name = "Decrypt")
        .setCoder(plaintextListCoder)

    val groupedDecryptedResults: PCollection<KV<JoinKeyIdentifier, List<@JvmWildcard Plaintext>>> =
      individualDecryptedResults
        .groupByKey("Group Decrypted Results by JoinKeyIdentifier")
        .map("Map to List of Plaintext") { kvOf(it.key, it.value.flatten()) }
        .setCoder(plaintextListCoder)

    val paddingQueryResults =
      groupedDecryptedResults.filter("Filter Padding Results") { it.key.isPaddingQuery }
    val realQueryResults =
      groupedDecryptedResults.filter("Filter Real Results") { !it.key.isPaddingQuery }

    val paddingQueryKeyedDecryptedEventDataSets =
      paddingQueryResults.map("Build Padding Nonce Results") {
        keyedDecryptedEventDataSet {
          plaintextJoinKeyAndId = joinKeyAndId { joinKeyIdentifier = it.key }
          decryptedEventData += it.value
        }
      }

    val keyedPlaintextJoinKeyAndIds =
      plaintextJoinKeyAndIds.keyBy("Key PlaintextJoinKeys By JoinKeyIdentifier") {
        it.joinKeyIdentifier
      }
    val realKeyedDecryptedEventDataSets =
      realQueryResults
        .strictOneToOneJoin(
          keyedPlaintextJoinKeyAndIds,
          name = "Join Decrypted Result to Plaintext Joinkeys",
        )
        .map("Map to KeyedDecryptedEventDataSet") {
          keyedDecryptedEventDataSet {
            plaintextJoinKeyAndId = it.value
            decryptedEventData += it.key
          }
        }

    return listOf(paddingQueryKeyedDecryptedEventDataSets, realKeyedDecryptedEventDataSets)
      .toPCollectionList()
      .flatten("Flatten Padding Nonces and Real Results")
  }

  companion object {
    val encryptedQueryResultsTag = TupleTag<EncryptedQueryResult>()
    val queryIdAndIdsTag = TupleTag<QueryIdAndId>()
    val decryptedJoinKeyAndIdsTag = TupleTag<JoinKeyAndId>()
    val plaintextJoinKeyAndIdsTag = TupleTag<JoinKeyAndId>()
  }
}

private class BuildDecryptQueryResultsParametersFn(
  private val keysView: PCollectionView<AsymmetricKeyPair>,
  private val compressionParametersView: PCollectionView<CompressionParameters>,
  private val parameters: Any,
  private val hkdfPepper: ByteString,
) :
  DoFn<
    KV<JoinKeyAndId?, Iterable<@JvmWildcard EncryptedQueryResult>?>,
    KV<JoinKeyIdentifier, DecryptQueryResultsParameters>,
  >() {
  private val metricsNamespace = "DecryptQueryResults"
  private val noResults = Metrics.counter(metricsNamespace, "no-results")
  private val paddingQueries = Metrics.counter(metricsNamespace, "padding-queries")

  @ProcessElement
  fun processElement(context: ProcessContext) {
    val decryptedJoinKeyAndId: JoinKeyAndId? = context.element().key // Null for padding queries
    val encryptedQueryResultsList: List<EncryptedQueryResult> =
      requireNotNull(context.element().value).toList()

    if (encryptedQueryResultsList.isEmpty()) {
      noResults.inc()
      return
    } else if (decryptedJoinKeyAndId == null) {
      paddingQueries.inc()
    }

    // For padding queries (i.e. those without a decryptedJoinKeyAndId) we use an empty JoinKey to
    // signal to the decryption algorithm that there is no AES encryption.
    val joinKey: JoinKey = decryptedJoinKeyAndId?.joinKey ?: joinKeyOf(ByteString.EMPTY)
    val joinKeyIdentifier: JoinKeyIdentifier =
      decryptedJoinKeyAndId?.joinKeyIdentifier ?: makePaddingQueryJoinKeyIdentifier()

    val keys = context.sideInput(keysView)
    val compressionParameters = context.sideInput(compressionParametersView)

    for (item in encryptedQueryResultsList) {
      val decryptParameters =
        DecryptQueryResultsParameters(
          parameters = this@BuildDecryptQueryResultsParametersFn.parameters,
          hkdfPepper = this@BuildDecryptQueryResultsParametersFn.hkdfPepper,
          serializedPublicKey = keys.serializedPublicKey,
          serializedPrivateKey = keys.serializedPrivateKey,
          compressionParameters = compressionParameters,
          decryptedJoinKey = joinKey,
          encryptedQueryResults = listOf(item),
        )
      context.output(kvOf(joinKeyIdentifier, decryptParameters))
    }
  }
}

private class DecryptResultsFn(private val queryResultsDecryptor: QueryResultsDecryptor) :
  DoFn<
    KV<JoinKeyIdentifier, DecryptQueryResultsParameters>,
    KV<JoinKeyIdentifier, List<@JvmWildcard Plaintext>>,
  >() {
  private val metricsNamespace = "DecryptQueryResults"
  private val decryptionTimes = Metrics.distribution(metricsNamespace, "decryption-times")
  private val outputCounts = Metrics.distribution(metricsNamespace, "output-counts")

  @ProcessElement
  fun processElement(context: ProcessContext) {
    val (decryptedResults, time) =
      withTime { queryResultsDecryptor.decryptQueryResults(context.element().value) }

    decryptionTimes.update(time.toNanos())
    outputCounts.update(decryptedResults.eventDataSetsCount.toLong())

    val joinKeyIdentifier = context.element().key

    for (eventDataSet in decryptedResults.eventDataSetsList) {
      context.output(kvOf(joinKeyIdentifier, eventDataSet.decryptedEventDataList))
    }
  }
}

data class DecryptQueryResultsParameters(
  val parameters: Any,
  val hkdfPepper: ByteString,
  val serializedPublicKey: ByteString,
  val serializedPrivateKey: ByteString,
  val compressionParameters: CompressionParameters,
  val decryptedJoinKey: JoinKey,
  val encryptedQueryResults: Iterable<EncryptedQueryResult>,
) : Serializable
