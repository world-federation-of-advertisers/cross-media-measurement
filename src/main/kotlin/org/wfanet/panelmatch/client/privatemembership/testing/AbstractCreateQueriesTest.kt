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

package org.wfanet.panelmatch.client.privatemembership.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertFailsWith
import org.apache.beam.sdk.metrics.MetricNameFilter
import org.apache.beam.sdk.metrics.MetricsFilter
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.joinKeyOf
import org.wfanet.panelmatch.client.common.panelistKeyOf
import org.wfanet.panelmatch.client.common.shardIdOf
import org.wfanet.panelmatch.client.privatemembership.CreateQueriesOutputs
import org.wfanet.panelmatch.client.privatemembership.CreateQueriesParameters
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.PanelistKeyAndJoinKey
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.QueryIdAndPanelistKey
import org.wfanet.panelmatch.client.privatemembership.createQueries
import org.wfanet.panelmatch.client.privatemembership.panelistKeyAndJoinKey
import org.wfanet.panelmatch.common.beam.join
import org.wfanet.panelmatch.common.beam.keyBy
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.parDo
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.beam.values
import org.wfanet.panelmatch.common.toByteString

@RunWith(JUnit4::class)
abstract class AbstractCreateQueriesTest : BeamTestBase() {
  private val panelistKeyAndJoinKeys by lazy {
    getPanelistKeyAndJoinKeys(
      53L to "abc",
      58L to "def",
      71L to "hij",
      85L to "klm",
      95L to "nop",
      99L to "qrs"
    )
  }

  abstract val privateMembershipSerializedParameters: ByteString
  abstract val privateMembershipCryptor: PrivateMembershipCryptor
  abstract val privateMembershipCryptorHelper: PrivateMembershipCryptorHelper

  private fun runWorkflow(
    privateMembershipCryptor: PrivateMembershipCryptor,
    parameters: CreateQueriesParameters,
  ): CreateQueriesOutputs {
    val keys = pcollectionViewOf("Create Keys", privateMembershipCryptor.generateKeys())
    return createQueries(panelistKeyAndJoinKeys, keys, parameters, privateMembershipCryptor)
  }

  @Test
  fun `Two Shards with no padding`() {
    val parameters =
      CreateQueriesParameters(
        numShards = 2,
        numBucketsPerShard = 5,
        maxQueriesPerShard = 12345,
        padQueries = false
      )
    val (panelistKeyQueryId, encryptedResults) = runWorkflow(privateMembershipCryptor, parameters)
    val decodedQueries =
      decodeEncryptedQueryBundle(privateMembershipCryptorHelper, encryptedResults)
    val panelistQueries = getPanelistQueries(decodedQueries, panelistKeyQueryId)
    assertThat(panelistQueries.values())
      .containsInAnyOrder(
        PanelistQuery(shard = 0, panelist = 53L, bucket = 0),
        PanelistQuery(shard = 1, panelist = 58L, bucket = 1),
        PanelistQuery(shard = 1, panelist = 71L, bucket = 3),
        PanelistQuery(shard = 1, panelist = 85L, bucket = 1),
        PanelistQuery(shard = 1, panelist = 95L, bucket = 2),
        PanelistQuery(shard = 0, panelist = 99L, bucket = 0)
      )
    assertFailsWith(NoSuchElementException::class) { runPipelineAndGetNumberOfDiscardedQueries() }
  }

  @Test
  fun `Two Shards with extra padding`() {
    val numShards = 2
    val totalQueriesPerShard = 10
    val numBucketsPerShard = 5
    val parameters =
      CreateQueriesParameters(
        numShards = numShards,
        numBucketsPerShard = numBucketsPerShard,
        maxQueriesPerShard = totalQueriesPerShard,
        padQueries = true
      )
    val (panelistKeyQueryId, encryptedQueries) = runWorkflow(privateMembershipCryptor, parameters)
    val decodedQueries =
      decodeEncryptedQueryBundle(privateMembershipCryptorHelper, encryptedQueries)
    val panelistQueries = getPanelistQueries(decodedQueries, panelistKeyQueryId)
    assertThat(panelistQueries.values())
      .containsInAnyOrder(
        PanelistQuery(0, 53L, 0),
        PanelistQuery(1, 58L, 1),
        PanelistQuery(1, 71L, 3),
        PanelistQuery(1, 85L, 1),
        PanelistQuery(1, 95L, 2),
        PanelistQuery(0, 99L, 0)
      )
    assertThat(decodedQueries.values()).satisfies { shardedQueries ->
      for (i in 0 until numShards) {
        val resultsPerShard = shardedQueries.filter { it.shardId == shardIdOf(i) }
        assertThat(resultsPerShard.count()).isEqualTo(totalQueriesPerShard)
      }
      null
    }
    assertThat(runPipelineAndGetNumberOfDiscardedQueries()).isEqualTo(0)
  }

  @Test
  fun `Two Shards with removed queries`() {
    val numShards = 2
    val totalQueriesPerShard = 3
    val numBucketsPerShard = 4
    val parameters =
      CreateQueriesParameters(
        numShards = numShards,
        numBucketsPerShard = numBucketsPerShard,
        maxQueriesPerShard = totalQueriesPerShard,
        padQueries = true
      )

    val (_, encryptedResults) = runWorkflow(privateMembershipCryptor, parameters)
    val decodedQueries =
      decodeEncryptedQueryBundle(privateMembershipCryptorHelper, encryptedResults)
    assertThat(decodedQueries.values()).satisfies { shardedQueries ->
      for (i in 0 until numShards) {
        val resultsPerShard = shardedQueries.filter { it.shardId == shardIdOf(i) }
        assertThat(resultsPerShard.count()).isEqualTo(totalQueriesPerShard)
      }
      null
    }
    assertThat(runPipelineAndGetNumberOfDiscardedQueries()).isEqualTo(1)
  }

  private fun runPipelineAndGetNumberOfDiscardedQueries(): Long {
    val pipelineResult = pipeline.run()
    pipelineResult.waitUntilFinish()
    val filter =
      MetricsFilter.builder()
        .addNameFilter(MetricNameFilter.named("CreateQueries", "discarded-queries-per-shard"))
        .build()
    val metrics = pipelineResult.metrics().queryMetrics(filter)
    return metrics.distributions.map { it.committed.max }.first()
  }

  private fun getPanelistKeyAndJoinKeys(
    vararg entries: Pair<Long, String>
  ): PCollection<PanelistKeyAndJoinKey> {
    return pcollectionOf(
      "Create PanelistKey+JoinKeys",
      *entries
        .map {
          panelistKeyAndJoinKey {
            panelistKey = panelistKeyOf(it.first)
            joinKey = joinKeyOf(it.second.toByteString())
          }
        }
        .toTypedArray()
    )
  }

  private fun decodeEncryptedQueryBundle(
    privateMembershipCryptorHelper: PrivateMembershipCryptorHelper,
    data: PCollection<EncryptedQueryBundle>
  ): PCollection<KV<QueryId, ShardedQuery>> {
    return data.parDo("Map to ShardedQuery") { encryptedQueriesData ->
      val shardedQueries =
        privateMembershipCryptorHelper.decodeEncryptedQueryBundle(
          EncryptedQueryBundle.parseFrom(encryptedQueriesData.serializedEncryptedQueries)
        )
      yieldAll(shardedQueries.map { kvOf(it.queryId, it) })
    }
  }

  private fun getPanelistQueries(
    decryptedQueries: PCollection<KV<QueryId, ShardedQuery>>,
    panelistKeyQueryId: PCollection<QueryIdAndPanelistKey>
  ): PCollection<KV<QueryId, PanelistQuery>> {
    return panelistKeyQueryId.keyBy { it.queryId }.join(decryptedQueries) {
      key: QueryId,
      panelistKeys: Iterable<QueryIdAndPanelistKey>,
      shardedQueries: Iterable<ShardedQuery> ->
      if (panelistKeys.count() > 0) {
        val queriesList = shardedQueries.toList()
        val panelistKeysList = panelistKeys.toList()

        val query =
          requireNotNull(queriesList.singleOrNull()) { "${queriesList.size} queries for $key" }

        val queryIdAndPanelistKey =
          requireNotNull(panelistKeysList.singleOrNull()) {
            "${panelistKeysList.size} of panelistKeys for $key"
          }

        val panelistQuery =
          PanelistQuery(query.shardId, queryIdAndPanelistKey.panelistKey, query.bucketId)
        yield(kvOf(key, panelistQuery))
      }
    }
  }
}
