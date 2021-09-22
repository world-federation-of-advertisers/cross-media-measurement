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
import kotlin.test.assertFailsWith
import org.apache.beam.sdk.metrics.MetricNameFilter
import org.apache.beam.sdk.metrics.MetricsFilter
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.privatemembership.CreateQueriesWorkflow
import org.wfanet.panelmatch.client.privatemembership.CreateQueriesWorkflow.Parameters
import org.wfanet.panelmatch.client.privatemembership.EncryptedQuery
import org.wfanet.panelmatch.client.privatemembership.JoinKey
import org.wfanet.panelmatch.client.privatemembership.PanelistKey
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipEncryptResponse
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.joinKeyOf
import org.wfanet.panelmatch.client.privatemembership.panelistKeyOf
import org.wfanet.panelmatch.client.privatemembership.shardIdOf
import org.wfanet.panelmatch.common.beam.join
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.beam.values
import org.wfanet.panelmatch.common.toByteString

@RunWith(JUnit4::class)
abstract class AbstractCreateQueriesWorkflowTest : BeamTestBase() {
  private val database by lazy {
    databaseOf(53L to "abc", 58L to "def", 71L to "hij", 85L to "klm", 95L to "nop", 99L to "qrs")
  }
  abstract val privateMembershipCryptor: PrivateMembershipCryptor
  abstract val privateMembershipCryptorHelper: PrivateMembershipCryptorHelper

  private fun runWorkflow(
    privateMembershipCryptor: PrivateMembershipCryptor,
    parameters: Parameters
  ): Pair<PCollection<KV<QueryId, PanelistKey>>, PCollection<PrivateMembershipEncryptResponse>> {
    return CreateQueriesWorkflow(
        parameters = parameters,
        privateMembershipCryptor = privateMembershipCryptor
      )
      .batchCreateQueries(database)
  }

  private fun getPanelistQueries(
    decryptedQueries: PCollection<KV<QueryId, ShardedQuery>>,
    panelistKeyQueryId: PCollection<KV<QueryId, PanelistKey>>
  ): PCollection<KV<QueryId, PanelistQuery>> {
    return panelistKeyQueryId.join(decryptedQueries) {
      key: QueryId,
      panelistKeys: Iterable<PanelistKey>,
      shardedQueries: Iterable<ShardedQuery> ->
      if (panelistKeys.count() > 0) {
        yield(
          kvOf(
            key,
            PanelistQuery(
              shardedQueries.single().shardId.id,
              panelistKeys.single().id,
              shardedQueries.single().bucketId.id
            )
          )
        )
      }
    }
  }

  @Test
  fun `Two Shards with no padding`() {
    val parameters = Parameters(numShards = 2, numBucketsPerShard = 5, totalQueriesPerShard = null)
    val (panelistKeyQueryId, encryptedResults) = runWorkflow(privateMembershipCryptor, parameters)
    val decodedQueries = privateMembershipCryptorHelper.decodeEncryptedQuery(encryptedResults)
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
    assertFailsWith(NoSuchElementException::class) { runPipelineAndGetNumberOfDiscardedQueries() }
  }

  @Test
  fun `Two Shards with extra padding`() {
    val numShards = 2
    val totalQueriesPerShard = 10
    val numBucketsPerShard = 5
    val parameters =
      Parameters(
        numShards = numShards,
        numBucketsPerShard = numBucketsPerShard,
        totalQueriesPerShard = totalQueriesPerShard
      )
    val (panelistKeyQueryId, encryptedResults) = runWorkflow(privateMembershipCryptor, parameters)
    val decodedQueries = privateMembershipCryptorHelper.decodeEncryptedQuery(encryptedResults)
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
    assertThat(encryptedResults).satisfies { encryptedResultsIterable ->
      val encryptedQueries: List<EncryptedQuery> =
        encryptedResultsIterable.flatMap { it.encryptedQueryList }
      for (i in 0 until numShards) {
        val resultsPerShard = encryptedQueries.filter { it.shardId == shardIdOf(i) }
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
      Parameters(
        numShards = numShards,
        numBucketsPerShard = numBucketsPerShard,
        totalQueriesPerShard = totalQueriesPerShard
      )

    val (_, encryptedResults) = runWorkflow(privateMembershipCryptor, parameters)
    assertThat(encryptedResults).satisfies { encryptedResultsIterable ->
      val encryptedQueries: List<EncryptedQuery> =
        encryptedResultsIterable.flatMap { it.encryptedQueryList }
      for (i in 0 until numShards) {
        val resultsPerShard = encryptedQueries.filter { it.shardId == shardIdOf(i) }
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
        .addNameFilter(
          MetricNameFilter.named(CreateQueriesWorkflow::class.java, "discarded-queries")
        )
        .build()
    val metrics = pipelineResult.metrics().queryMetrics(filter)
    return metrics.distributions.map { it.committed.max }.first()
  }

  private fun databaseOf(
    vararg entries: Pair<Long, String>
  ): PCollection<KV<PanelistKey, JoinKey>> {
    return pcollectionOf(
      "Create Database",
      *entries
        .map { kvOf(panelistKeyOf(it.first), joinKeyOf(it.second.toByteString())) }
        .toTypedArray()
    )
  }
}
