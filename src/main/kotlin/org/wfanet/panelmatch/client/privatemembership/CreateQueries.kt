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
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.Flatten
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionTuple
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.values.TupleTag
import org.wfanet.panelmatch.client.common.bucketIdOf
import org.wfanet.panelmatch.client.common.joinKeyIdentifierOf
import org.wfanet.panelmatch.client.common.queryIdOf
import org.wfanet.panelmatch.client.common.unencryptedQueryOf
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyIdentifier
import org.wfanet.panelmatch.common.beam.filter
import org.wfanet.panelmatch.common.beam.groupByKey
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.parDo
import org.wfanet.panelmatch.common.beam.values
import org.wfanet.panelmatch.common.crypto.AsymmetricKeyPair
import org.wfanet.panelmatch.common.withTime

private val FAKE_JOIN_KEY_ID = ByteString.EMPTY

/**
 * Implements a query creation engine in Apache Beam that encrypts a query so that it can later be
 * expanded by another party using oblivious query expansion.
 */
fun createQueries(
  lookupKeyAndIds: PCollection<LookupKeyAndId>,
  privateMembershipKeys: PCollectionView<AsymmetricKeyPair>,
  parameters: CreateQueriesParameters,
  privateMembershipCryptor: PrivateMembershipCryptor
): CreateQueriesOutputs {
  val tuple =
    lookupKeyAndIds.apply(
      "Create Queries",
      CreateQueries(privateMembershipKeys, parameters, privateMembershipCryptor)
    )
  return CreateQueriesOutputs(
    queryIdMap = tuple[CreateQueries.queryIdAndIdTag],
    encryptedQueryBundles = tuple[CreateQueries.encryptedQueryBundlesTag]
  )
}

data class CreateQueriesOutputs(
  val queryIdMap: PCollection<QueryIdAndId>,
  val encryptedQueryBundles: PCollection<EncryptedQueryBundle>
)

private class CreateQueries(
  private val privateMembershipKeys: PCollectionView<AsymmetricKeyPair>,
  private val parameters: CreateQueriesParameters,
  private val privateMembershipCryptor: PrivateMembershipCryptor,
) : PTransform<PCollection<LookupKeyAndId>, PCollectionTuple>() {

  override fun expand(input: PCollection<LookupKeyAndId>): PCollectionTuple {
    val queriesByShard = shardLookupKeys(input)
    val paddedQueriesByShard = addPaddedQueries(queriesByShard)
    val unencryptedQueriesByShard = buildUnencryptedQueries(paddedQueriesByShard)
    val queryIdToIdsMapping = extractRealQueryIdAndId(unencryptedQueriesByShard)
    val encryptedQueryBundles = encryptQueries(unencryptedQueriesByShard, privateMembershipKeys)
    return PCollectionTuple.of(queryIdAndIdTag, queryIdToIdsMapping)
      .and(encryptedQueryBundlesTag, encryptedQueryBundles)
  }

  /** Determines shard and bucket for a [LookupKey]. */
  private fun shardLookupKeys(
    lookupKeys: PCollection<LookupKeyAndId>
  ): PCollection<KV<ShardId, Iterable<@JvmWildcard BucketQuery>>> {
    val bucketing =
      Bucketing(
        numShards = parameters.numShards,
        numBucketsPerShard = parameters.numBucketsPerShard
      )
    return lookupKeys
      .map("Map to ShardId") { lookupKeyAndId: LookupKeyAndId ->
        val (shardId, bucketId) = bucketing.apply(lookupKeyAndId.lookupKey.key)
        kvOf(shardId, BucketQuery(lookupKeyAndId.joinKeyIdentifier, shardId, bucketId))
      }
      .groupByKey("Group by Shard")
  }

  /** Wrapper function to add in the necessary number of padded queries */
  private fun addPaddedQueries(
    queries: PCollection<KV<ShardId, Iterable<@JvmWildcard BucketQuery>>>
  ): PCollection<KV<ShardId, Iterable<@JvmWildcard BucketQuery>>> {
    if (!parameters.padQueries) return queries
    val totalQueriesPerShard = parameters.maxQueriesPerShard
    return queries.parDo(
      EqualizeQueriesPerShardFn(totalQueriesPerShard),
      name = "Equalize Queries per Shard"
    )
  }

  /**
   * Assigns a unique, random QueryId to each [BucketQuery].
   *
   * The range [0, Int.MAX_VALUE) is partitioned into a sub-range per shard and then the queries in
   * each shard are randomly assigned distinct ids from the sub-range.
   *
   * For example, if there are 10 shards, then queries from the first shard are assigned ids from
   * [0, x), queries from the second shard from [x, 2 * x), where x = Int.MAX_VALUE / 10.
   *
   * This is efficient because it can process each shard in parallel.
   */
  private fun buildUnencryptedQueries(
    queries: PCollection<KV<ShardId, Iterable<@JvmWildcard BucketQuery>>>
  ): PCollection<KV<ShardId, List<FullUnencryptedQuery>>> {
    val queryIdUpperBound = Int.MAX_VALUE / parameters.numShards
    return queries
      .map("Build UnencryptedQueries") { kv ->
        val shardId = kv.key
        val offset = shardId.id * queryIdUpperBound
        val queryIds = generateQueryIds(queryIdUpperBound)
        val unencryptedQueries =
          kv.value.map { query: BucketQuery ->
            val queryId = queryIdOf(queryIds.next() + offset)
            val unencryptedQuery = unencryptedQueryOf(shardId, query.bucketId, queryId)
            FullUnencryptedQuery(query.joinKeyIdentifier, unencryptedQuery)
          }
        kvOf(shardId, unencryptedQueries)
      }
      .setCoder(
        // TODO: figure out why an explicit coder is needed here.
        // Beam is unable to infer a coder for "? extends FullUnencryptedQuery".
        // This is not urgent -- it is very typical to explicitly set Coders in Apache Beam.
        KvCoder.of(
          ProtoCoder.of(ShardId::class.java),
          ListCoder.of(SerializableCoder.of(FullUnencryptedQuery::class.java))
        )
      )
  }

  /** Filter out fake queries and return [QueryIdAndId]s. */
  private fun extractRealQueryIdAndId(
    fullUnencryptedQueries: PCollection<KV<ShardId, List<FullUnencryptedQuery>>>
  ): PCollection<QueryIdAndId> {
    return fullUnencryptedQueries
      .values("Drop ShardIds")
      .apply("Flatten", Flatten.iterables())
      .filter("Filter out padded queries") { it.joinKeyIdentifier.id != FAKE_JOIN_KEY_ID }
      .map { fullUnencryptedQuery ->
        queryIdAndId {
          queryId = fullUnencryptedQuery.unencryptedQuery.queryId
          joinKeyIdentifier = fullUnencryptedQuery.joinKeyIdentifier
        }
      }
  }

  /** Batch gets the oblivious queries grouped by [ShardId]. */
  private fun encryptQueries(
    unencryptedQueries: PCollection<KV<ShardId, List<FullUnencryptedQuery>>>,
    privateMembershipKeys: PCollectionView<AsymmetricKeyPair>
  ): PCollection<EncryptedQueryBundle> {
    return unencryptedQueries.apply(
      "Encrypt Queries per Shard",
      ParDo.of(EncryptQueriesFn(this.privateMembershipCryptor, privateMembershipKeys))
        .withSideInputs(privateMembershipKeys)
    )
  }

  companion object {
    val queryIdAndIdTag = TupleTag<QueryIdAndId>()
    val encryptedQueryBundlesTag = TupleTag<EncryptedQueryBundle>()
  }
}

/**
 * Bucket queries are for join keys that have a shard and bucket id assigned but don't have a query
 * id yet.
 */
private data class BucketQuery(
  val joinKeyIdentifier: JoinKeyIdentifier,
  val shardId: ShardId,
  val bucketId: BucketId
) : Serializable

/** An Unencrypted Query (shard id, bucket id, query id) tied back to a joinKeyIdentifier. */
private data class FullUnencryptedQuery(
  val joinKeyIdentifier: JoinKeyIdentifier,
  val unencryptedQuery: UnencryptedQuery
) : Serializable

private const val METRIC_NAMESPACE: String = "CreateQueries"

private class EncryptQueriesFn(
  private val cryptor: PrivateMembershipCryptor,
  private val keys: PCollectionView<AsymmetricKeyPair>
) : DoFn<KV<ShardId, List<@JvmWildcard FullUnencryptedQuery>>, EncryptedQueryBundle>() {
  /** Time (in nanos) to encrypt each query. */
  private val encryptionTimesDistribution =
    Metrics.distribution(METRIC_NAMESPACE, "encryption-times")

  /** Size (in bytes) of each serialized encryptedQueries. */
  private val encryptedQueriesSizeDistribution =
    Metrics.distribution(METRIC_NAMESPACE, "encrypted-queries-sizes")

  @ProcessElement
  fun processElement(context: ProcessContext) {
    val unencryptedQueries = context.element().value.map { it.unencryptedQuery }

    val (encryptedQueries, time) =
      withTime { cryptor.encryptQueries(unencryptedQueries, context.sideInput(keys)) }

    encryptionTimesDistribution.update(time.toNanos())
    encryptedQueriesSizeDistribution.update(encryptedQueries.size().toLong())

    val shardId = unencryptedQueries.firstOrNull()?.shardId ?: return

    val bundle = encryptedQueryBundle {
      this.shardId = shardId
      queryIds += unencryptedQueries.map { it.queryId }
      serializedEncryptedQueries = encryptedQueries
    }

    context.output(bundle)
  }
}

/**
 * Adds or deletes queries from sharded data until it is the desired size. We keep track of which
 * queries are fake in order to avoid attempting to decrypt them later.
 */
private class EqualizeQueriesPerShardFn(private val totalQueriesPerShard: Int) :
  DoFn<
    KV<ShardId, Iterable<@JvmWildcard BucketQuery>>,
    KV<ShardId, Iterable<@JvmWildcard BucketQuery>>>() {
  /**
   * Number of discarded Queries. If unacceptably high, the totalQueriesPerShard parameter should be
   * increased.
   */
  private val discardedQueriesDistribution =
    Metrics.distribution(METRIC_NAMESPACE, "discarded-queries-per-shard")

  /** Number of padding queries added to each shard. */
  private val paddingQueriesDistribution =
    Metrics.distribution(METRIC_NAMESPACE, "padding-queries-per-shard")

  @ProcessElement
  fun processElement(context: ProcessContext) {
    val kv = context.element()
    val allQueries = kv.value.toList()

    val queryCountDelta = allQueries.size - totalQueriesPerShard
    discardedQueriesDistribution.update(maxOf(0L, queryCountDelta.toLong()))

    if (queryCountDelta >= 0) {
      context.output(kvOf(kv.key, allQueries.take(totalQueriesPerShard)))
      return
    }

    paddingQueriesDistribution.update(-queryCountDelta.toLong())
    val paddingQueries =
      List(-queryCountDelta) {
        // TODO: If we add in query mitigation, the BucketId should be set to the fake bucket
        BucketQuery(joinKeyIdentifierOf(FAKE_JOIN_KEY_ID), kv.key, bucketIdOf(0))
      }

    context.output(kvOf(kv.key, allQueries + paddingQueries))
  }
}
