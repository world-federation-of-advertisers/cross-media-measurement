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
import org.apache.beam.sdk.transforms.join.CoGbkResult
import org.apache.beam.sdk.transforms.join.CoGroupByKey
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionTuple
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.values.TupleTag
import org.wfanet.panelmatch.client.common.bucketOf
import org.wfanet.panelmatch.client.common.databaseShardOf
import org.wfanet.panelmatch.common.beam.keyBy
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.withTime

/** Implements a batch query engine in Apache Beam using homomorphic encryption. */
fun evaluateQueries(
  database: PCollection<DatabaseEntry>,
  queryBundles: PCollection<EncryptedQueryBundle>,
  serializedPublicKey: PCollectionView<ByteString>,
  paddingNonces: PCollectionView<Map<QueryId, PaddingNonce>>,
  parameters: EvaluateQueriesParameters,
  queryEvaluator: QueryEvaluator,
): PCollection<EncryptedQueryResult> {
  return PCollectionTuple.of(EvaluateQueries.databaseTag, database)
    .and(EvaluateQueries.queryBundlesTag, queryBundles)
    .apply(
      "Evaluate Queries",
      EvaluateQueries(parameters, queryEvaluator, serializedPublicKey, paddingNonces),
    )
}

private class EvaluateQueries(
  private val parameters: EvaluateQueriesParameters,
  private val queryEvaluator: QueryEvaluator,
  private val serializedPublicKey: PCollectionView<ByteString>,
  private val paddingNonces: PCollectionView<Map<QueryId, PaddingNonce>>,
) : PTransform<PCollectionTuple, PCollection<EncryptedQueryResult>>() {

  override fun expand(input: PCollectionTuple): PCollection<EncryptedQueryResult> {
    val database = input[databaseTag]
    val queryBundles = input[queryBundlesTag]

    val bucketing = Bucketing(parameters.numShards, parameters.numBucketsPerShard)

    val databaseByShard: PCollection<KV<ShardId, Bucket>> =
      database.map("Form Database Buckets by Shard") { databaseEntry ->
        val (shardId, bucketId) = bucketing.apply(databaseEntry.lookupKey.key)
        kvOf(shardId, bucketOf(bucketId, listOf(databaseEntry.encryptedEntry.data)))
      }

    val queryBundlesByShard = queryBundles.keyBy("Key Queries by Shard") { it.shardId }

    return KeyedPCollectionTuple.of(EvaluateQueriesForShardFn.databaseTag, databaseByShard)
      .and(EvaluateQueriesForShardFn.queryBundlesTag, queryBundlesByShard)
      .apply("Join Database and Queries", CoGroupByKey.create())
      .apply(
        "Evaluate Queries per Shard",
        ParDo.of(
            EvaluateQueriesForShardFn(
              parameters.maxQueriesPerShard,
              queryEvaluator,
              serializedPublicKey,
              paddingNonces,
            )
          )
          .withSideInputs(serializedPublicKey, paddingNonces),
      )
  }

  companion object {
    val databaseTag = TupleTag<DatabaseEntry>()
    val queryBundlesTag = TupleTag<EncryptedQueryBundle>()
  }
}

private class EvaluateQueriesForShardFn(
  private val maxQueriesPerShard: Int,
  private val queryEvaluator: QueryEvaluator,
  private val serializedPublicKey: PCollectionView<ByteString>,
  private val paddingNonces: PCollectionView<Map<QueryId, PaddingNonce>>,
) : DoFn<KV<ShardId, CoGbkResult>, EncryptedQueryResult>() {
  private val metricsNamespace = "EvaluateQueries"

  /** Distribution of the number of queries per shard. */
  private val queryCountsDistribution = Metrics.distribution(metricsNamespace, "query-counts")

  /** Distribution of the time it takes [queryEvaluator] to run. */
  private val queryEvaluatorTimes = Metrics.distribution(metricsNamespace, "query-evaluator-times")

  /** Count of the number of queries belonging to a shard with no buckets. */
  private val missingShardsCounter = Metrics.counter(metricsNamespace, "missing-shards")

  /** Count of the number of shards without any assigned queries. */
  private val noQueriesCounter = Metrics.counter(metricsNamespace, "no-queries")

  /**
   * Distribution of the combined serialized sizes of the [DatabaseShard] and all
   * [EncryptedQueryBundle]s.
   */
  private val totalSizeDistribution = Metrics.distribution(metricsNamespace, "total-sizes")

  /** Distribution of the summed serialized sizes of all [EncryptedQueryBundle]s for a shard. */
  private val combinedEncryptedQueryBundleSizeDistribution =
    Metrics.distribution(metricsNamespace, "combined-encrypted-query-bundle-sizes")

  /** Distribution of the serialized sizes of each [EncryptedQueryBundle]. */
  private val queryBundleSizeDistribution =
    Metrics.distribution(metricsNamespace, "encrypted-query-bundle-sizes")

  /** Distribution of the serialized sizes of each [DatabaseShard]. */
  private val databaseShardSizeDistribution =
    Metrics.distribution(metricsNamespace, "database-shard-sizes")

  /** Distribution of the number of buckets per [DatabaseShard]. */
  private val bucketCountDistribution = Metrics.distribution(metricsNamespace, "bucket-counts")

  /** Distribution of the serialized sizes of each [Bucket]. */
  private val bucketSizeDistribution = Metrics.distribution(metricsNamespace, "bucket-sizes")

  @ProcessElement
  fun processElement(context: ProcessContext) {
    val joinResult = context.element().value
    val buckets = joinResult.getAll(databaseTag).toList()
    val queries = joinResult.getAll(queryBundlesTag).toList()

    validateAndCountInputs(buckets, queries)
    if (buckets.isEmpty() || queries.isEmpty()) return

    val combinedBuckets =
      buckets
        .groupBy { it.bucketId }
        .map { (bucketId, buckets) ->
          bucketOf(bucketId, buckets.flatMap { it.contents.itemsList })
        }
    val shard = databaseShardOf(context.element().key, combinedBuckets)

    val publicKey = context.sideInput(serializedPublicKey)
    val nonces = context.sideInput(paddingNonces)

    val (results, time) =
      withTime { queryEvaluator.executeQueries(listOf(shard), queries, nonces, publicKey) }
    queryEvaluatorTimes.update(time.toNanos())

    for (result in results) {
      context.output(result)
    }
  }

  private fun validateAndCountInputs(buckets: List<Bucket>, queries: List<EncryptedQueryBundle>) {
    for (query in queries) {
      queryBundleSizeDistribution.update(query.serializedSize.toLong())
    }

    for (bucket in buckets) {
      bucketSizeDistribution.update(bucket.serializedSize.toLong())
    }

    val shardSize = buckets.sumOf { it.serializedSize.toLong() }
    databaseShardSizeDistribution.update(shardSize)

    val queriesSize = queries.sumOf { it.serializedSize.toLong() }
    totalSizeDistribution.update(shardSize + queriesSize)
    combinedEncryptedQueryBundleSizeDistribution.update(queriesSize)
    bucketCountDistribution.update(buckets.size.toLong())

    val numQueries = queries.sumOf { it.queryIdsCount }
    queryCountsDistribution.update(numQueries.toLong())
    require(numQueries <= maxQueriesPerShard) {
      "Shard has $numQueries queries ($maxQueriesPerShard allowed)"
    }

    if (queries.isEmpty()) noQueriesCounter.inc()
    if (buckets.isEmpty()) missingShardsCounter.inc()
  }

  companion object {
    val queryBundlesTag = TupleTag<EncryptedQueryBundle>()
    val databaseTag = TupleTag<Bucket>()
  }
}
