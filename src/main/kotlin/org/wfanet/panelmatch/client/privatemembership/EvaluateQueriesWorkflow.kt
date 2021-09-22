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

import java.io.Serializable
import java.lang.IllegalArgumentException
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.common.beam.groupByKey
import org.wfanet.panelmatch.common.beam.join
import org.wfanet.panelmatch.common.beam.keyBy
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.parDo

/**
 * Implements a batch query engine in Apache Beam using homomorphic encryption.
 *
 * @param parameters tuning knobs for the workflow
 * @param queryEvaluator implementation of lower-level homomorphic operations
 */
class EvaluateQueriesWorkflow(
  private val parameters: Parameters,
  private val queryEvaluator: QueryEvaluator
) : Serializable {

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
    queryBundles: PCollection<QueryBundle>
  ): PCollection<Result> {
    val shardedDatabase: PCollection<KV<ShardId, DatabaseShard>> = shardDatabase(database)

    val queriesByShard = queryBundles.keyBy("Key QueryBundles by Shard") { it.shardId }

    return queryShards(shardedDatabase, queriesByShard)
  }

  /** Joins the inputs to execute the queries on the appropriate shards. */
  private fun queryShards(
    shardedDatabase: PCollection<KV<ShardId, DatabaseShard>>,
    queriesByShard: PCollection<KV<ShardId, QueryBundle>>
  ): PCollection<Result> {
    return shardedDatabase.join(queriesByShard, name = "Join Database and queryMetadata") {
      key: ShardId,
      shards: Iterable<DatabaseShard>,
      queries: Iterable<QueryBundle> ->
      val queriesList = queries.toList()

      val numQueries = queriesList.sumBy { it.queryIdsCount }
      require(numQueries <= parameters.maxQueriesPerShard) {
        "Shard $key has $numQueries queries (${parameters.maxQueriesPerShard} allowed) $queriesList"
      }

      if (numQueries > 0) {
        val shardsList = shards.toList()
        // TODO(@efoxepstein): consider throwing an error when the size is 0, too.
        when (shardsList.size) {
          0 -> return@join
          1 -> yieldAll(queryEvaluator.executeQueries(shardsList, queriesList))
          else -> throw IllegalArgumentException("Too many DatabaseShards for shard $key")
        }
      }
    }
  }

  /** Splits the database into [DatabaseShard]s of appropriate size. */
  private fun shardDatabase(
    database: PCollection<KV<DatabaseKey, Plaintext>>
  ): PCollection<KV<ShardId, DatabaseShard>> {
    val bucketing = Bucketing(parameters.numShards, parameters.numBucketsPerShard)
    return database
      .keyBy {
        val (shardId, bucketId) = bucketing.apply(it.key.id)
        kvOf(shardId, bucketId)
      }
      .groupByKey("Group by Shard and Bucket")
      .map { kv ->
        val combinedValues = kv.value.map { it.value.payload }.flatten()
        kvOf(kv.key.key, bucketOf(kv.key.value, combinedValues))
      }
      .groupByKey("Group by Shard")
      .map { kvOf(it.key, databaseShardOf(it.key, it.value.toList())) }
      .parDo(DatabaseShardSizeObserver())
  }
}

/**
 * This makes a Distribution metric of the number of bytes in each shard's buckets' payloads.
 *
 * Note that this is a different value than the size of the DatabaseShard as a serialized proto.
 */
private class DatabaseShardSizeObserver :
  DoFn<KV<ShardId, DatabaseShard>, KV<ShardId, DatabaseShard>>() {
  private val sizeDistribution =
    Metrics.distribution(EvaluateQueriesWorkflow::class.java, "database-shard-sizes")

  @ProcessElement
  fun processElement(context: ProcessContext) {
    val bucketsList = context.element().value.bucketsList
    val totalSize = bucketsList.sumOf { it.payload.size().toLong() }
    sizeDistribution.update(totalSize)
    context.output(context.element())
  }
}
