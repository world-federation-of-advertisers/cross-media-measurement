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
import com.google.common.truth.Truth.assertWithMessage
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.random.Random
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollection
import org.junit.Test
import org.wfanet.panelmatch.client.common.databaseEntryOf
import org.wfanet.panelmatch.client.common.encryptedEntryOf
import org.wfanet.panelmatch.client.common.lookupKeyOf
import org.wfanet.panelmatch.client.common.paddingNonceOf
import org.wfanet.panelmatch.client.common.queryIdOf
import org.wfanet.panelmatch.client.privatemembership.BucketId
import org.wfanet.panelmatch.client.privatemembership.Bucketing
import org.wfanet.panelmatch.client.privatemembership.DatabaseEntry
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.EvaluateQueriesParameters
import org.wfanet.panelmatch.client.privatemembership.PaddingNonce
import org.wfanet.panelmatch.client.privatemembership.QueryEvaluator
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.ShardId
import org.wfanet.panelmatch.client.privatemembership.evaluateQueries
import org.wfanet.panelmatch.client.privatemembership.paddingNonceMapCoder
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat

/** Base test class for testing the full pipeline, including a specific [QueryEvaluator]. */
abstract class AbstractEvaluateQueriesEndToEndTest : BeamTestBase() {
  /** Provides a test subject. */
  abstract fun makeQueryEvaluator(parameters: EvaluateQueriesParameters): QueryEvaluator

  /** Provides a helper for the test subject. */
  abstract fun makeHelper(parameters: EvaluateQueriesParameters): QueryEvaluatorTestHelper

  @Test
  fun endToEnd() {
    for (numShards in listOf(1, 10, 100)) {
      for (numBucketsPerShard in listOf(1, 10, 1000)) {
        val parameters =
          EvaluateQueriesParameters(
            numShards = numShards,
            numBucketsPerShard = numBucketsPerShard,
            maxQueriesPerShard = 1000,
          )
        runEndToEndTest(parameters)
      }
    }
  }

  private fun runEndToEndTest(parameters: EvaluateQueriesParameters) {
    val queryEvaluator = makeQueryEvaluator(parameters)
    val helper = makeHelper(parameters)

    val random = Random(seed = 12345L)
    val keys: List<Long> = (0 until 10).map { random.nextLong() }
    assertThat(keys).containsNoDuplicates() // Sanity check: 10 different keys

    val rawDatabase: Map<Long, ByteString> =
      keys.associateWith { "<this is the payload for $it>".toByteStringUtf8() }

    val database: List<DatabaseEntry> =
      rawDatabase.map { databaseEntryOf(lookupKeyOf(it.key), encryptedEntryOf(it.value)) }
    val databasePCollection: PCollection<DatabaseEntry> =
      pipeline.apply("Create Database", Create.of(database))

    val rawMatchingQueries = keys.take(3).mapIndexed { i, key -> key to queryIdOf(i) }
    val rawMissingQueries = (0 until 3).map { i -> random.nextLong() to queryIdOf(3 + i) }
    val rawQueries = rawMatchingQueries + rawMissingQueries
    assertThat(rawQueries.map { it.first }).containsNoDuplicates() // Sanity check

    val expectedResults: List<String> =
      rawMatchingQueries.map { rawDatabase.getValue(it.first).toStringUtf8() }

    val bucketing =
      Bucketing(
        numShards = parameters.numShards,
        numBucketsPerShard = parameters.numBucketsPerShard,
      )
    val bucketsAndShardsToQuery: List<Pair<Pair<ShardId, BucketId>, QueryId>> =
      rawQueries.map { bucketing.apply(it.first) to it.second }

    val queryBundles: List<EncryptedQueryBundle> =
      bucketsAndShardsToQuery
        .groupBy { it.first.first }
        .map { (shard, entries) ->
          helper.makeQueryBundle(shard, entries.map { it.second to it.first.second })
        }

    val queryBundlesPCollection = pipeline.apply("Create QueryBundles", Create.of(queryBundles))

    val paddingNonces: Map<QueryId, PaddingNonce> =
      rawQueries.associate {
        it.second to paddingNonceOf("padding-nonce-for-${it.second.id}".toByteStringUtf8())
      }

    val results: PCollection<EncryptedQueryResult> =
      evaluateQueries(
        databasePCollection,
        queryBundlesPCollection,
        pcollectionViewOf("Create SerializedPublicKey", helper.serializedPublicKey),
        pcollectionViewOf("Create Padding Nonces", paddingNonces, coder = paddingNonceMapCoder),
        parameters,
        queryEvaluator,
      )

    assertThat(results).satisfies {
      // First, we decode each result and then split each bucket up into individual values.
      // This is to handle the case where multiple database entries fall into the same bucket.
      //
      // Then, since the same bucket could be selected by multiple queries, we convert all the
      // individual results into a set.
      //
      // Finally, we compare the unique results with the expected results.
      val uniqueResults =
        it
          .flatMap { result -> helper.decodeResultData(result).itemsList }
          .map { item -> item.toStringUtf8() }
          .toSet()
      assertWithMessage("with $parameters")
        .that(uniqueResults)
        .containsAtLeastElementsIn(expectedResults)
      null
    }
  }
}
