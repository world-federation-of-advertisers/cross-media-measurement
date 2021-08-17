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
import java.util.BitSet
import kotlin.math.abs
import kotlin.random.Random
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.common.beam.groupByKey
import org.wfanet.panelmatch.common.beam.keyBy
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.parDo
import org.wfanet.panelmatch.common.beam.values

/**
 * Implements a query creation engine in Apache Beam that encrypts a query so that it can later be
 * expanded by another party using oblivious query expansion.
 *
 * @param parameters tuning knobs for the workflow
 * @param privateMembershipCryptor implementation of lower-level oblivious query expansion and
 * result decryption
 */
class CreateQueriesWorkflow(
  private val parameters: Parameters,
  private val privateMembershipCryptor: PrivateMembershipCryptor
) : Serializable {

  /**
   * Tuning knobs for the [BatchCreationWorkflow].
   *
   * @property numShards the number of shards to split the data into
   * @property numBucketsPerShard the number of buckets each shard can have
   * @property totalQueriesPerShard [Int?] pads the number of queries per shard to be this number.
   * If the number of queries is larger than [totalQueriesPerShard], then queries in that shard are
   * culled down to [totalQueriesPerShard]. Null signfies no additional padding/culling should take
   * place. TODO: Implement totalQueriesPerShard
   */
  data class Parameters
  private constructor(
    val numShards: Int,
    val numBucketsPerShard: Int,
    val totalQueriesPerShard: Int?,
    val numQueries: Int
  ) : Serializable {
    init {
      require(numShards > 0)
      require(numBucketsPerShard > 0)
      totalQueriesPerShard?.let { require(totalQueriesPerShard > numBucketsPerShard) }
    }
    constructor(
      numShards: Int,
      numBucketsPerShard: Int,
      totalQueriesPerShard: Int?
    ) : this(
      numShards,
      numBucketsPerShard,
      totalQueriesPerShard,
      if (totalQueriesPerShard == null) {
        100000
      } else {
        numShards * totalQueriesPerShard
      }
    )
  }

  /** Creates [EncryptQueriesResponse] on [data]. */
  fun batchCreateQueries(
    data: PCollection<KV<PanelistKey, JoinKey>>
  ): Pair<PCollection<KV<QueryId, PanelistKey>>, PCollection<EncryptQueriesResponse>> {
    val mappedData = mapToQueryId(data)
    // TODO add in padded queries after we get the unencrypted queries
    val unencryptedQueries = buildUnencryptedQueryRequest(mappedData)
    val panelistToQueryIdMapping = getPanelistToQueryMapping(mappedData)
    return Pair(panelistToQueryIdMapping, getPrivateMembershipQueries(unencryptedQueries))
  }

  /**
   * Maps each [PanelistKey] to a unique [QueryId] using an iterator. Works well as long as total
   * collection size is less than ~90% of the mapped [QueryId] space (currently 32 bits). The
   * current iterator uses a BitSet that only supports nonnegative integers which further reduces
   * the mapped space to 16 bits.
   */
  private fun mapToQueryId(
    data: PCollection<KV<PanelistKey, JoinKey>>
  ): PCollection<KV<KV<PanelistKey, QueryId>, JoinKey>> {
    return data.keyBy { 1 }.groupByKey().parDo {
      val queryIds: Iterator<Int> = iterator {
        val seen = BitSet()
        while (seen.cardinality() < parameters.numQueries) {
          val id = abs(Random.nextInt())
          if (!seen.get(id)) {
            seen.set(id)
            yield(id)
          }
        }
      }
      it
        .value
        .asSequence()
        .mapIndexed { index, kv ->
          require(index < parameters.numQueries) { "Too many queries" }
          kvOf(kvOf(requireNotNull(kv.key), queryIdOf(queryIds.next())), requireNotNull(kv.value))
        }
        .also { yieldAll(it) }
    }
  }

  /** Maps each [PanelistKey] to a unique [QueryId]. */
  private fun getPanelistToQueryMapping(
    data: PCollection<KV<KV<PanelistKey, QueryId>, JoinKey>>
  ): PCollection<KV<QueryId, PanelistKey>> {
    return data.map("Map of PanelistKey to QueryId") { kvOf(it.key.value, it.key.key) }
  }

  /** Builds [EncryptedQuery] from the encrypted data join keys of [JoinKey]. */
  private fun buildUnencryptedQueryRequest(
    data: PCollection<KV<KV<PanelistKey, QueryId>, JoinKey>>
  ): PCollection<KV<ShardId, UnencryptedQuery>> {
    val bucketing = Bucketing(parameters.numShards, parameters.numBucketsPerShard)
    return data.map(name = "Map to UnencryptedQuery") {
      val (shardId, bucketId) = bucketing.hashAndApply(it.value)
      kvOf(shardId, unencryptedQueryOf(shardId, bucketId, it.key.value))
    }
  }

  /** Batch gets the oblivious queries grouped by [ShardId]. */
  private fun getPrivateMembershipQueries(
    data: PCollection<KV<ShardId, UnencryptedQuery>>
  ): PCollection<EncryptQueriesResponse> {
    return data
      .groupByKey("Group by Shard")
      .map<KV<ShardId, Iterable<UnencryptedQuery>>, KV<ShardId, EncryptQueriesResponse>>(
        name = "Map to EncryptQueriesResponse"
      ) {
        val encryptQueriesRequest =
          EncryptQueriesRequest.newBuilder().addAllUnencryptedQuery(it.value).build()
        kvOf(it.key, privateMembershipCryptor.encryptQueries(encryptQueriesRequest))
      }
      .values("Extract Results")
  }
}
