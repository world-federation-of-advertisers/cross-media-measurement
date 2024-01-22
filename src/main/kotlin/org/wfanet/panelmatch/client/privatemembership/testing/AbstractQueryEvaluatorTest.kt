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
import org.junit.Test
import org.wfanet.panelmatch.client.common.bucketIdOf
import org.wfanet.panelmatch.client.common.bucketOf
import org.wfanet.panelmatch.client.common.databaseShardOf
import org.wfanet.panelmatch.client.common.paddingNonceOf
import org.wfanet.panelmatch.client.common.queryIdOf
import org.wfanet.panelmatch.client.common.shardIdOf
import org.wfanet.panelmatch.client.privatemembership.Bucket
import org.wfanet.panelmatch.client.privatemembership.BucketContents
import org.wfanet.panelmatch.client.privatemembership.DatabaseShard
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.PaddingNonce
import org.wfanet.panelmatch.client.privatemembership.QueryEvaluator
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.bucketContents
import org.wfanet.panelmatch.client.privatemembership.testing.QueryEvaluatorTestHelper.DecodedResult

/** Tests for [QueryEvaluator]s. */
abstract class AbstractQueryEvaluatorTest {
  protected val shardCount: Int = 128
  protected val bucketsPerShardCount: Int = 16

  /** Provides a test subject. */
  protected abstract val evaluator: QueryEvaluator

  /** Provides a helper for the test subject. */
  protected abstract val helper: QueryEvaluatorTestHelper

  @Test
  fun `executeQueries on multiple shards with multiple QueryBundles`() {
    val database =
      listOf(
        databaseShardOf(shard = 100, buckets = listOf(0, 1, 2)),
        databaseShardOf(shard = 101, buckets = listOf(3, 4, 5)),
      )
    val queryBundles =
      listOf(
        encryptedQueryBundleOf(shard = 100, queries = listOf(500 to 0, 501 to 1, 502 to 3)),
        encryptedQueryBundleOf(shard = 100, queries = listOf(503 to 9)),
        encryptedQueryBundleOf(
          shard = 101,
          queries = listOf(504 to 0, 505 to 1, 506 to 2, 507 to 3),
        ),
      )
    val paddingNonces = makePaddingNonces(500..507)

    val results =
      evaluator.executeQueries(database, queryBundles, paddingNonces, helper.serializedPublicKey)

    assertThat(results.map { it.queryId to helper.decodeResultData(it) })
      .containsExactly(
        queryIdOf(500) to makeFakeBucketData(bucket = 0, shard = 100),
        queryIdOf(501) to makeFakeBucketData(bucket = 1, shard = 100),
        queryIdOf(502) to bucketContents {},
        queryIdOf(503) to bucketContents {},
        queryIdOf(504) to bucketContents {},
        queryIdOf(505) to bucketContents {},
        queryIdOf(506) to bucketContents {},
        queryIdOf(507) to makeFakeBucketData(bucket = 3, shard = 101),
      )
  }

  @Test
  fun `executeQueries same bucket with multiple shards`() {
    val database =
      listOf(
        databaseShardOf(shard = 100, buckets = listOf(1)),
        databaseShardOf(shard = 101, buckets = listOf(1)),
      )

    val queryBundles =
      listOf(
        encryptedQueryBundleOf(shard = 100, queries = listOf(500 to 1)),
        encryptedQueryBundleOf(shard = 101, queries = listOf(501 to 1)),
      )
    val paddingNonces = makePaddingNonces(listOf(500, 501))

    val results =
      evaluator.executeQueries(database, queryBundles, paddingNonces, helper.serializedPublicKey)
    assertThat(results.map { helper.decodeResult(it) })
      .containsExactly(
        DecodedResult(500, makeFakeBucketData(bucket = 1, shard = 100)),
        DecodedResult(501, makeFakeBucketData(bucket = 1, shard = 101)),
      )
  }

  private fun encryptedQueryBundleOf(
    shard: Int,
    queries: List<Pair<Int, Int>>,
  ): EncryptedQueryBundle {
    return helper.makeQueryBundle(
      shardIdOf(shard),
      queries.map { queryIdOf(it.first) to bucketIdOf(it.second) },
    )
  }
}

private fun bucketOf(id: Int, data: ByteString): Bucket {
  return bucketOf(bucketIdOf(id), listOf(data))
}

private fun databaseShardOf(shard: Int, buckets: List<Int>): DatabaseShard {
  return databaseShardOf(
    shardIdOf(shard),
    buckets.map { bucketOf(it, makeFakeBucketData(it, shard).itemsList.single()) },
  )
}

private fun makeFakeBucketData(bucket: Int, shard: Int): BucketContents {
  return bucketContents { items += "bucket:$bucket-shard:$shard".toByteStringUtf8() }
}

private fun makePaddingNonces(queryIds: Iterable<Int>): Map<QueryId, PaddingNonce> {
  return queryIds.associate {
    queryIdOf(it) to paddingNonceOf("padding-nonce-$it".toByteStringUtf8())
  }
}
