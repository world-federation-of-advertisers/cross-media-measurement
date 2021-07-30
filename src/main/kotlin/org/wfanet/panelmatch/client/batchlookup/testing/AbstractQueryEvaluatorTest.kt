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

package org.wfanet.panelmatch.client.batchlookup.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertFailsWith
import org.junit.Test
import org.wfanet.panelmatch.client.batchlookup.Bucket
import org.wfanet.panelmatch.client.batchlookup.DatabaseShard
import org.wfanet.panelmatch.client.batchlookup.QueryBundle
import org.wfanet.panelmatch.client.batchlookup.QueryEvaluator
import org.wfanet.panelmatch.client.batchlookup.Result
import org.wfanet.panelmatch.client.batchlookup.bucketIdOf
import org.wfanet.panelmatch.client.batchlookup.bucketOf
import org.wfanet.panelmatch.client.batchlookup.databaseShardOf
import org.wfanet.panelmatch.client.batchlookup.queryIdOf
import org.wfanet.panelmatch.client.batchlookup.shardIdOf
import org.wfanet.panelmatch.client.batchlookup.testing.QueryEvaluatorTestHelper.DecodedResult

/** Tests for [QueryEvaluator]s. */
abstract class AbstractQueryEvaluatorTest {
  /** Provides a test subject. */
  abstract val evaluator: QueryEvaluator

  /** Provides a helper for the test subject. */
  abstract val helper: QueryEvaluatorTestHelper

  @Test
  fun `executeQueries on multiple shards with multiple QueryBundles`() {
    val database =
      listOf(
        databaseShardOf(shard = 100, buckets = listOf(0, 1, 2)),
        databaseShardOf(shard = 101, buckets = listOf(3, 4, 5))
      )
    val queryBundles =
      listOf(
        queryBundleOf(shard = 100, queries = listOf(500 to 0, 501 to 1, 502 to 3)),
        queryBundleOf(shard = 100, queries = listOf(503 to 9)),
        queryBundleOf(shard = 102, queries = listOf(504 to 0, 505 to 1, 506 to 2, 507 to 3))
      )
    val results = evaluator.executeQueries(database, queryBundles)
    assertThat(results.map { it.queryMetadata.queryId to helper.decodeResultData(it) })
      .containsExactly(
        queryIdOf(500) to makeFakeBucketData(bucket = 0, shard = 100),
        queryIdOf(501) to makeFakeBucketData(bucket = 1, shard = 100),
        queryIdOf(502) to ByteString.EMPTY,
        queryIdOf(503) to ByteString.EMPTY
      )
  }

  @Test
  fun `executeQueries same bucket with multiple shards`() {
    val database =
      listOf(
        databaseShardOf(shard = 100, buckets = listOf(1)),
        databaseShardOf(shard = 101, buckets = listOf(1))
      )

    val queryBundles =
      listOf(
        queryBundleOf(shard = 100, queries = listOf(500 to 1)),
        queryBundleOf(shard = 101, queries = listOf(501 to 1))
      )

    val results = evaluator.executeQueries(database, queryBundles)
    assertThat(results.map { helper.decodeResult(it) })
      .containsExactly(
        DecodedResult(500, makeFakeBucketData(bucket = 1, shard = 100)),
        DecodedResult(501, makeFakeBucketData(bucket = 1, shard = 101))
      )
  }

  @Test
  fun `combineResults empty input`() {
    assertFailsWith<IllegalArgumentException> { evaluator.combineResults(emptySequence()) }
  }

  @Test
  fun `combineResults mismatching queries`() {
    assertFailsWith<IllegalArgumentException> {
      runCombineResults(resultOf(1, ByteString.EMPTY), resultOf(2, ByteString.EMPTY))
    }
  }

  @Test
  fun `combineResults single result`() {
    val queryId = 5

    assertThat(runCombineResults(resultOf(queryId, ByteString.EMPTY)))
      .isEqualTo(DecodedResult(queryId, ByteString.EMPTY))

    val rawPayload = "some-raw-payload".toByteString()
    assertThat(runCombineResults(resultOf(queryId, rawPayload)))
      .isEqualTo(DecodedResult(queryId, rawPayload))
  }

  @Test
  fun `combineResults multiple results`() {
    val queryId = 5

    val emptyResult = resultOf(queryId, ByteString.EMPTY)
    val payload = "some-payload".toByteString()
    val nonEmptyResult = resultOf(queryId, payload)

    assertThat(runCombineResults(emptyResult, nonEmptyResult, emptyResult, emptyResult))
      .isEqualTo(DecodedResult(queryId, payload))

    assertThat(runCombineResults(emptyResult, emptyResult, emptyResult))
      .isEqualTo(DecodedResult(queryId, ByteString.EMPTY))

    // It is not fully defined what happens when combining multiple non-empty results.
    // This checks that it does not throw:
    runCombineResults(nonEmptyResult, nonEmptyResult)
  }

  private fun runCombineResults(vararg results: Result): DecodedResult {
    return helper.decodeResult(evaluator.combineResults(results.asSequence()))
  }

  private fun queryBundleOf(shard: Int, queries: List<Pair<Int, Int>>): QueryBundle {
    return helper.makeQueryBundle(
      shardIdOf(shard),
      queries.map { queryIdOf(it.first) to bucketIdOf(it.second) }
    )
  }

  private fun resultOf(query: Int, rawPayload: ByteString): Result {
    return helper.makeResult(queryIdOf(query), rawPayload)
  }
}

private fun bucketOf(id: Int, data: ByteString): Bucket {
  return bucketOf(bucketIdOf(id), data)
}

private fun databaseShardOf(shard: Int, buckets: List<Int>): DatabaseShard {
  return databaseShardOf(
    shardIdOf(shard),
    buckets.map { bucketOf(it, makeFakeBucketData(it, shard)) }
  )
}

private fun makeFakeBucketData(bucket: Int, shard: Int): ByteString {
  return "bucket:$bucket-shard:$shard".toByteString()
}

private fun String.toByteString(): ByteString {
  return ByteString.copyFromUtf8(this)
}
