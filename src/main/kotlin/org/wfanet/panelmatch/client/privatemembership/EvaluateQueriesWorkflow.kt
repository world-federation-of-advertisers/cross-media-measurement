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
import java.lang.IllegalArgumentException
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.join.CoGbkResult
import org.apache.beam.sdk.transforms.join.CoGroupByKey
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.values.TupleTag
import org.wfanet.panelmatch.common.beam.groupByKey
import org.wfanet.panelmatch.common.beam.keyBy
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.values
import org.wfanet.panelmatch.common.withTime

/**
 * Implements a batch query engine in Apache Beam using homomorphic encryption.
 *
 * TODO: consider passing in `queryEvaluator` as a parameter to `batchEvaluateQueries`
 *
 * @param parameters tuning knobs for the workflow
 * @param queryEvaluator implementation of lower-level homomorphic operations
 */
class EvaluateQueriesWorkflow(
  private val parameters: Parameters,
  private val queryEvaluator: QueryEvaluator
) {

  /**
   * Tuning knobs for the [EvaluateQueriesWorkflow].
   *
   * @property numShards the number of shards to split the database into
   * @property numBucketsPerShard the number of buckets each shard can have
   * @property maxQueriesPerShard the number of queries each shard can have -- this is a safeguard
   * against malicious behavior
   */
  data class Parameters(
    val numShards: Int,
    val numBucketsPerShard: Int,
    val maxQueriesPerShard: Int
  ) : Serializable {
    init {
      require(numShards > 0)
      require(numBucketsPerShard > 0)
      require(maxQueriesPerShard > 0)
    }
  }

  /** Evaluates [queryBundles] on [database]. */
  fun batchEvaluateQueries(
    database: PCollection<KV<DatabaseKey, Plaintext>>,
    queryBundles: PCollection<EncryptedQueryBundle>,
    serializedPublicKey: PCollectionView<ByteString>
  ): PCollection<EncryptedQueryResult> {
    val bucketing = Bucketing(parameters.numShards, parameters.numBucketsPerShard)
    val databaseByShard = database.apply("Shard Database", ShardDatabase(bucketing))

    val queriesByShard = queryBundles.keyBy("Key QueryBundles by Shard") { it.shardId }

    @Suppress("NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")
    return KeyedPCollectionTuple.of(ExecuteQueries.databaseTag, databaseByShard)
      .and(ExecuteQueries.queriesTag, queriesByShard)
      .apply(
        "Execute Queries",
        ExecuteQueries(queryEvaluator, serializedPublicKey, parameters.maxQueriesPerShard)
      )
  }
}

private class ShardDatabase(private val bucketing: Bucketing) :
  PTransform<PCollection<KV<DatabaseKey, Plaintext>>, PCollection<KV<ShardId, DatabaseShard>>>() {

  override fun expand(
    input: PCollection<KV<DatabaseKey, Plaintext>>
  ): PCollection<KV<ShardId, DatabaseShard>> {
    return input
      .map("Key by Shard") {
        val (shardId, bucketId) = bucketing.apply(it.key.id)
        kvOf(shardId, bucketOf(bucketId, listOf(it.value.payload)))
      }
      .groupByKey("Group by Shard")
      .map("Map Buckets to DatabaseShard") { kv ->
        val buckets =
          kv.value.groupBy { it.bucketId }.map {
            bucketOf(it.key, it.value.flatMap { bucket -> bucket.contents.itemsList })
          }
        kvOf(kv.key, databaseShardOf(kv.key, buckets))
      }
  }
}

private class ExecuteQueries(
  private val queryEvaluator: QueryEvaluator,
  private val serializedPublicKey: PCollectionView<ByteString>,
  private val maxQueriesPerShard: Int
) : PTransform<KeyedPCollectionTuple<ShardId>, PCollection<EncryptedQueryResult>>() {

  override fun expand(input: KeyedPCollectionTuple<ShardId>): PCollection<EncryptedQueryResult> {
    @Suppress("NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")
    return input
      .apply("Join Database and Queries", CoGroupByKey.create())
      .values("Drop ShardIds")
      .apply(
        "Execute Queries for Shard",
        ParDo.of(ExecuteQueriesForShardFn(maxQueriesPerShard, queryEvaluator, serializedPublicKey))
          .withSideInputs(serializedPublicKey)
      )
  }

  companion object {
    val queriesTag = TupleTag<EncryptedQueryBundle>()
    val databaseTag = TupleTag<DatabaseShard>()
  }
}

private class ExecuteQueriesForShardFn(
  private val maxQueriesPerShard: Int,
  private val queryEvaluator: QueryEvaluator,
  private val serializedPublicKey: PCollectionView<ByteString>
) : DoFn<CoGbkResult, EncryptedQueryResult>() {
  private val metricsClass = EvaluateQueriesWorkflow::class.java

  /** Distribution of the number of queries per shard. */
  private val queryCountsDistribution = Metrics.distribution(metricsClass, "query-counts")

  /** Distribution of the time it takes [queryEvaluator] to run. */
  private val queryEvaluatorTimes = Metrics.distribution(metricsClass, "query-evaluator-times")

  /** Count of the number of queries belonging to a shard with no buckets. */
  private val missingShardsCounter = Metrics.counter(metricsClass, "missing-shards")

  /** Count of the number of shards without any assigned queries. */
  private val noQueriesCounter = Metrics.counter(metricsClass, "no-queries")

  /**
   * Distribution of the combined serialized sizes of the [DatabaseShard] and all
   * [EncryptedQueryBundle]s.
   */
  private val totalSizeDistribution = Metrics.distribution(metricsClass, "total-sizes")

  /** Distribution of the summed serialized sizes of all [EncryptedQueryBundle]s for a shard. */
  private val combinedEncryptedQueryBundleSizeDistribution =
    Metrics.distribution(metricsClass, "combined-encrypted-query-bundle-sizes")

  /** Distribution of the serialized sizes of each [EncryptedQueryBundle]. */
  private val queryBundleSizeDistribution =
    Metrics.distribution(metricsClass, "encrypted-query-bundle-sizes")

  /** Distribution of the serialized sizes of each [DatabaseShard]. */
  private val databaseShardSizeDistribution =
    Metrics.distribution(metricsClass, "database-shard-sizes")

  /** Distribution of the number of buckets per [DatabaseShard]. */
  private val bucketCountDistribution = Metrics.distribution(metricsClass, "bucket-counts")

  /** Distribution of the serialized sizes of each [Bucket]. */
  private val bucketSizeDistribution = Metrics.distribution(metricsClass, "bucket-sizes")

  @ProcessElement
  fun processElement(context: ProcessContext) {
    val shards = context.element().getAll(ExecuteQueries.databaseTag).toList()
    val queries = context.element().getAll(ExecuteQueries.queriesTag).toList()
    validateAndCountInputs(shards, queries)
    if (shards.isEmpty() || queries.isEmpty()) return

    val publicKey = context.sideInput(serializedPublicKey)

    val (results, time) = withTime { queryEvaluator.executeQueries(shards, queries, publicKey) }
    queryEvaluatorTimes.update(time.toNanos())

    results.forEach(context::output)
  }

  private fun validateAndCountInputs(
    shards: List<DatabaseShard>,
    queries: List<EncryptedQueryBundle>
  ) {
    for (query in queries) {
      queryBundleSizeDistribution.update(query.serializedSize.toLong())
    }

    for (shard in shards) {
      for (bucket in shard.bucketsList) {
        bucketSizeDistribution.update(bucket.serializedSize.toLong())
      }
    }

    val shardSize = shards.sumOf { it.serializedSize.toLong() }
    val queriesSize = queries.sumOf { it.serializedSize.toLong() }
    totalSizeDistribution.update(shardSize + queriesSize)
    databaseShardSizeDistribution.update(shardSize)
    combinedEncryptedQueryBundleSizeDistribution.update(queriesSize)
    bucketCountDistribution.update(shards.sumOf { it.bucketsCount.toLong() })

    val numQueries = queries.sumBy { it.queryIdsCount }
    queryCountsDistribution.update(numQueries.toLong())
    require(numQueries <= maxQueriesPerShard) {
      "Shard has $numQueries queries ($maxQueriesPerShard allowed)"
    }

    when {
      queries.isEmpty() -> noQueriesCounter.inc()
      shards.isEmpty() -> missingShardsCounter.inc()
      shards.size > 1 -> throw IllegalArgumentException("Too many DatabaseShards for shard")
    }
  }
}
