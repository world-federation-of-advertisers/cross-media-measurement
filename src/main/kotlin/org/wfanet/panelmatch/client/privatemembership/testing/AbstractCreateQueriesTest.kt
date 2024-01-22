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
import com.google.protobuf.kotlin.toByteStringUtf8
import java.io.Serializable
import kotlin.test.assertFailsWith
import org.apache.beam.sdk.metrics.MetricNameFilter
import org.apache.beam.sdk.metrics.MetricsFilter
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.bucketIdOf
import org.wfanet.panelmatch.client.common.joinKeyIdentifierOf
import org.wfanet.panelmatch.client.common.lookupKeyOf
import org.wfanet.panelmatch.client.common.shardIdOf
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyIdentifier
import org.wfanet.panelmatch.client.privatemembership.BucketId
import org.wfanet.panelmatch.client.privatemembership.CreateQueriesOutputs
import org.wfanet.panelmatch.client.privatemembership.CreateQueriesParameters
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.LookupKeyAndId
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.QueryIdAndId
import org.wfanet.panelmatch.client.privatemembership.ShardId
import org.wfanet.panelmatch.client.privatemembership.createQueries
import org.wfanet.panelmatch.client.privatemembership.isPaddingQuery
import org.wfanet.panelmatch.client.privatemembership.lookupKeyAndId
import org.wfanet.panelmatch.common.beam.count
import org.wfanet.panelmatch.common.beam.flatMap
import org.wfanet.panelmatch.common.beam.join
import org.wfanet.panelmatch.common.beam.keyBy
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.beam.values

@RunWith(JUnit4::class)
abstract class AbstractCreateQueriesTest : BeamTestBase() {
  private val lookupKeyAndIds by lazy {
    createLookupKeysAndIdsOf(
      53L to "abc",
      58L to "def",
      71L to "hij",
      85L to "klm",
      95L to "nop",
      99L to "qrs",
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
    return createQueries(lookupKeyAndIds, keys, parameters, privateMembershipCryptor)
  }

  @Test
  fun `Two Shards with no padding`() {
    val parameters =
      CreateQueriesParameters(
        numShards = 2,
        numBucketsPerShard = 5,
        maxQueriesPerShard = 12345,
        padQueries = false,
      )
    val (queryIdAndJoinKeys, encryptedResults) = runWorkflow(privateMembershipCryptor, parameters)
    val decodedQueries =
      decodeEncryptedQueryBundle(privateMembershipCryptorHelper, encryptedResults)
    val panelistQueries = getPanelistQueries(decodedQueries, queryIdAndJoinKeys)
    assertThat(panelistQueries.values())
      .containsInAnyOrder(
        PanelistQuery(shard = 1, bucket = 1, joinKeyIdentifier = "abc"),
        PanelistQuery(shard = 0, bucket = 4, joinKeyIdentifier = "def"),
        PanelistQuery(shard = 1, bucket = 0, joinKeyIdentifier = "hij"),
        PanelistQuery(shard = 1, bucket = 2, joinKeyIdentifier = "klm"),
        PanelistQuery(shard = 1, bucket = 2, joinKeyIdentifier = "nop"),
        PanelistQuery(shard = 1, bucket = 4, joinKeyIdentifier = "qrs"),
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
        padQueries = true,
      )
    val (queryIdAndJoinKeys, encryptedQueries) = runWorkflow(privateMembershipCryptor, parameters)
    val decodedQueries =
      decodeEncryptedQueryBundle(privateMembershipCryptorHelper, encryptedQueries)
    val panelistQueries = getPanelistQueries(decodedQueries, queryIdAndJoinKeys)
    assertThat(panelistQueries.values())
      .containsInAnyOrder(
        PanelistQuery(shard = 1, bucket = 1, joinKeyIdentifier = "abc"),
        PanelistQuery(shard = 0, bucket = 4, joinKeyIdentifier = "def"),
        PanelistQuery(shard = 1, bucket = 0, joinKeyIdentifier = "hij"),
        PanelistQuery(shard = 1, bucket = 2, joinKeyIdentifier = "klm"),
        PanelistQuery(shard = 1, bucket = 2, joinKeyIdentifier = "nop"),
        PanelistQuery(shard = 1, bucket = 4, joinKeyIdentifier = "qrs"),
      )
    assertThat(decodedQueries.values()).satisfies { shardedQueries ->
      for (i in 0 until numShards) {
        assertThat(shardedQueries.count { it.shardId.id == i }).isEqualTo(totalQueriesPerShard)
      }
      null
    }
    assertThat(runPipelineAndGetNumberOfDiscardedQueries()).isEqualTo(0)
  }

  @Test
  fun addsPaddingQueriesToMissingShards() {
    val numShards = 7 // Needs to exceed the number of lookup keys and ids.

    assertThat(lookupKeyAndIds.count()).satisfies {
      assertThat(it.single()).isLessThan(numShards)
      null
    }

    val totalQueriesPerShard = 10
    val numBucketsPerShard = 5
    val parameters =
      CreateQueriesParameters(
        numShards = numShards,
        numBucketsPerShard = numBucketsPerShard,
        maxQueriesPerShard = totalQueriesPerShard,
        padQueries = true,
      )
    val (queryIdAndJoinKeys, encryptedQueries, discardedJoinKeys) =
      runWorkflow(privateMembershipCryptor, parameters)
    val decodedQueries =
      decodeEncryptedQueryBundle(privateMembershipCryptorHelper, encryptedQueries)
    assertThat(discardedJoinKeys).satisfies { it ->
      assertThat(it.single().joinKeyIdentifiersList).isEmpty()
      null
    }
    assertThat(getPanelistQueries(decodedQueries, queryIdAndJoinKeys)).satisfies { panelistQueries
      ->
      val joinKeyIdentifiers = panelistQueries.map { it.value.joinKeyIdentifier.id.toStringUtf8() }
      assertThat(joinKeyIdentifiers).containsExactly("abc", "def", "hij", "klm", "nop", "qrs")
      null
    }

    assertThat(decodedQueries.values()).satisfies { shardedQueries ->
      for (i in 0 until numShards) {
        assertThat(shardedQueries.count { it.shardId.id == i }).isEqualTo(totalQueriesPerShard)
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
        padQueries = true,
      )

    val (_, encryptedResults, discardedJoinKeys) = runWorkflow(privateMembershipCryptor, parameters)
    val decodedQueries =
      decodeEncryptedQueryBundle(privateMembershipCryptorHelper, encryptedResults)
    assertThat(discardedJoinKeys).satisfies { it ->
      assertThat(it.single().joinKeyIdentifiersList).hasSize(2)
      null
    }
    assertThat(decodedQueries.values()).satisfies { shardedQueries ->
      for (i in 0 until numShards) {
        assertThat(shardedQueries.count { it.shardId.id == i }).isEqualTo(totalQueriesPerShard)
      }
      null
    }
    assertThat(runPipelineAndGetNumberOfDiscardedQueries()).isEqualTo(2)
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

  private fun createLookupKeysAndIdsOf(
    vararg entries: Pair<Long, String>
  ): PCollection<LookupKeyAndId> {
    return pcollectionOf(
      "Create LookupKeys+Ids",
      entries.map {
        lookupKeyAndId {
          lookupKey = lookupKeyOf(it.first)
          joinKeyIdentifier = joinKeyIdentifierOf(it.second.toByteStringUtf8())
        }
      },
    )
  }

  private fun decodeEncryptedQueryBundle(
    privateMembershipCryptorHelper: PrivateMembershipCryptorHelper,
    encryptedQueryBundles: PCollection<EncryptedQueryBundle>,
  ): PCollection<KV<QueryId, ShardedQuery>> {
    return encryptedQueryBundles
      .flatMap("Map to ShardedQuery") { encryptedQueryBundle ->
        privateMembershipCryptorHelper.decodeEncryptedQueryBundle(encryptedQueryBundle)
      }
      .keyBy { it.queryId }
  }

  private fun getPanelistQueries(
    decryptedQueries: PCollection<KV<QueryId, ShardedQuery>>,
    queryIdAndIds: PCollection<QueryIdAndId>,
  ): PCollection<KV<QueryId, PanelistQuery>> {
    return queryIdAndIds
      .keyBy { it.queryId }
      .join(decryptedQueries) {
        key: QueryId,
        queryIdAndIdsIterable: Iterable<QueryIdAndId>,
        shardedQueries: Iterable<ShardedQuery> ->
        val queryIdAndIdsList = queryIdAndIdsIterable.toList()
        if (queryIdAndIdsList.isEmpty()) {
          return@join
        }
        val queriesList = shardedQueries.toList()

        val query =
          requireNotNull(queriesList.singleOrNull()) { "${queriesList.size} queries for $key" }

        val queryIdAndId =
          requireNotNull(queryIdAndIdsList.singleOrNull()) {
            "${queryIdAndIdsList.size} of queryIdAndIds for $key"
          }

        if (queryIdAndId.joinKeyIdentifier.isPaddingQuery) {
          return@join
        }

        val panelistQuery =
          PanelistQuery(query.shardId, query.bucketId, queryIdAndId.joinKeyIdentifier)
        yield(kvOf(key, panelistQuery))
      }
  }
}

private data class PanelistQuery(
  val shardId: ShardId,
  val bucketId: BucketId,
  val joinKeyIdentifier: JoinKeyIdentifier,
) : Serializable {
  constructor(
    shard: Int,
    bucket: Int,
    joinKeyIdentifier: String,
  ) : this(
    shardIdOf(shard),
    bucketIdOf(bucket),
    joinKeyIdentifierOf(joinKeyIdentifier.toByteStringUtf8()),
  )
}
