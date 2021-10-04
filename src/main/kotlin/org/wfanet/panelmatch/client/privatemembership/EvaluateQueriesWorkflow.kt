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
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.ListCoder
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.common.beam.groupByKey
import org.wfanet.panelmatch.common.beam.join
import org.wfanet.panelmatch.common.beam.keyBy
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.parDo
import org.wfanet.panelmatch.common.beam.parDoWithSideInput

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
    val shardedDatabase: PCollection<KV<ShardId, DatabaseShard>> = shardDatabase(database)

    val queriesByShard = queryBundles.keyBy("Key QueryBundles by Shard") { it.shardId }

    return queryShards(shardedDatabase, queriesByShard, serializedPublicKey)
  }

  /** Joins the inputs to execute the queries on the appropriate shards. */
  private fun queryShards(
    shardedDatabase: PCollection<KV<ShardId, DatabaseShard>>,
    queriesByShard: PCollection<KV<ShardId, EncryptedQueryBundle>>,
    serializedPublicKey: PCollectionView<ByteString>
  ): PCollection<EncryptedQueryResult> {
    val shardsAndQueries: PCollection<KV<List<DatabaseShard>, List<EncryptedQueryBundle>>> =
      shardedDatabase.join(queriesByShard, name = "Join Database and Queries") {
        _: ShardId,
        shards,
        queries ->
        yield(kvOf(shards.toList(), queries.toList()))
      }

    // shardsAndQueries requires an explicit coder to be set because of differences in how Java and
    // Kotlin handle type inference around Lists/Iterables.
    // TODO: investigate if @JvmWildcard can be used instead
    @Suppress("NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")
    shardsAndQueries.coder =
      KvCoder.of(
        ListCoder.of(ProtoCoder.of(DatabaseShard::class.java)),
        ListCoder.of(ProtoCoder.of(EncryptedQueryBundle::class.java))
      )

    // Local references because EvaluatorQueriesWorkflow is not serializable.
    val maxQueriesPerShard = parameters.maxQueriesPerShard
    val queryEvaluator = this.queryEvaluator

    return shardsAndQueries.parDoWithSideInput(serializedPublicKey, name = "Execute Queries") {
      kv: KV<List<DatabaseShard>, List<EncryptedQueryBundle>>,
      publicKey: ByteString ->
      val shards = kv.key
      val queries = kv.value

      val numQueries = queries.sumBy { it.queryIdsCount }
      require(numQueries <= maxQueriesPerShard) {
        "Shard has $numQueries queries ($maxQueriesPerShard allowed)"
      }

      if (numQueries > 0) {
        // TODO(@efoxepstein): consider throwing an error when the size is 0, too.
        when (shards.size) {
          0 -> return@parDoWithSideInput
          1 -> yieldAll(queryEvaluator.executeQueries(shards, queries, publicKey))
          else -> throw IllegalArgumentException("Too many DatabaseShards for shard")
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
      .keyBy("Key by Shard & Bucket") {
        val (shardId, bucketId) = bucketing.apply(it.key.id)
        kvOf(shardId, bucketId)
      }
      .groupByKey("Group by Shard and Bucket")
      .map("Map Shard to Bucket") { kv ->
        val combinedValues = kv.value.map { it.value.payload }.flatten()
        kvOf(kv.key.key, bucketOf(kv.key.value, combinedValues))
      }
      .groupByKey("Group by Shard")
      .map("Map Shard to DatabaseShard") {
        kvOf(it.key, databaseShardOf(it.key, it.value.toList()))
      }
      .parDo(DatabaseShardSizeObserver(), name = "Observe shard sizes")
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
