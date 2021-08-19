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
import kotlin.random.Random
import org.apache.beam.sdk.transforms.Create
import org.junit.Test
import org.wfanet.panelmatch.client.privatemembership.Bucketing
import org.wfanet.panelmatch.client.privatemembership.DatabaseKey
import org.wfanet.panelmatch.client.privatemembership.EvaluateQueriesWorkflow
import org.wfanet.panelmatch.client.privatemembership.EvaluateQueriesWorkflow.Parameters
import org.wfanet.panelmatch.client.privatemembership.Plaintext
import org.wfanet.panelmatch.client.privatemembership.QueryBundle
import org.wfanet.panelmatch.client.privatemembership.QueryEvaluator
import org.wfanet.panelmatch.client.privatemembership.databaseKeyOf
import org.wfanet.panelmatch.client.privatemembership.plaintextOf
import org.wfanet.panelmatch.client.privatemembership.queryIdOf
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat

/** Base test class for testing the full pipeline, including a specific [QueryEvaluator]. */
abstract class AbstractEvaluateQueriesWorkflowEndToEndTest : BeamTestBase() {
  /** Provides a test subject. */
  abstract val queryEvaluator: QueryEvaluator

  /** Provides a helper for the test subject. */
  abstract val helper: QueryEvaluatorTestHelper

  @Test
  fun endToEnd() {
    for (numShards in listOf(1, 10, 100)) {
      for (numBucketsPerShard in listOf(1, 10, 100, 1000)) {
        for (subshardSizeBytes in listOf(500, 1000, 100000)) {
          val parameters =
            Parameters(
              numShards = numShards,
              numBucketsPerShard = numBucketsPerShard,
              subshardSizeBytes = subshardSizeBytes
            )
          runEndToEndTest(parameters)
        }
      }
    }
  }

  private fun runEndToEndTest(parameters: Parameters) {
    val random = Random(seed = 12345L)
    val keys: List<Long> = (0 until 10).map { random.nextLong() }
    assertThat(keys).containsNoDuplicates() // Sanity check: 10 different keys

    val rawDatabase: Map<Long, ByteString> =
      keys.associateWith { ByteString.copyFromUtf8("<this is the payload for $it>") }

    val database: Map<DatabaseKey, Plaintext> =
      rawDatabase.mapKeys { databaseKeyOf(it.key) }.mapValues { plaintextOf(it.value) }
    val databasePCollection = pipeline.apply("Create Database", Create.of(database))

    val rawMatchingQueries = keys.take(3).mapIndexed { i, key -> key to queryIdOf(i) }
    val rawMissingQueries = (0 until 3).map { i -> random.nextLong() to queryIdOf(3 + i) }
    val rawQueries = rawMatchingQueries + rawMissingQueries
    assertThat(rawQueries.map { it.first }).containsNoDuplicates() // Sanity check

    val expectedResults: List<String> =
      rawMatchingQueries.map { rawDatabase[it.first]!!.toStringUtf8() }

    val bucketing =
      Bucketing(
        numShards = parameters.numShards,
        numBucketsPerShard = parameters.numBucketsPerShard
      )
    val bucketsAndShardsToQuery = rawQueries.map { bucketing.apply(it.first) to it.second }
    val queryBundles: List<QueryBundle> =
      bucketsAndShardsToQuery.groupBy { it.first.first }.map { (shard, entries) ->
        helper.makeQueryBundle(shard, entries.map { it.second to it.first.second })
      }
    val queryBundlesPCollection = pipeline.apply("Create QueryBundles", Create.of(queryBundles))

    val workflow = EvaluateQueriesWorkflow(parameters, queryEvaluator)
    val results = workflow.batchEvaluateQueries(databasePCollection, queryBundlesPCollection)
    val localHelper = helper // For Beam's serialization

    assertThat(results).satisfies {
      // First, we decode each result and then split each bucket up into individual values.
      // This is to handle the case where multiple database entries fall into the same bucket.
      //
      // Then, since the same bucket could be selected by multiple queries, we convert all of
      // the individual results into a set.
      //
      // Finally, we compare the unique results with the expected results.
      val uniqueResults =
        it
          .map { result -> localHelper.decodeResultData(result).toStringUtf8() }
          .flatMap { decodedResult -> splitConcatenatedPayloads(decodedResult) }
          .toSet()
      assertWithMessage("with $parameters")
        .that(uniqueResults)
        .containsAtLeastElementsIn(expectedResults)
      null
    }
  }
}

/**
 * Splits [combinedPayloads] into individual payloads.
 *
 * We assume that each individual payload's first and last characters are '<' and '>', respectively.
 */
private fun splitConcatenatedPayloads(combinedPayloads: String): List<String> {
  return Regex("(<[^>]+>)").findAll(combinedPayloads).map { match -> match.groupValues[1] }.toList()
}
