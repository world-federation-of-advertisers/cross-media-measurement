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

import com.google.protobuf.ByteString
import com.google.protobuf.ListValue
import org.wfanet.panelmatch.client.privatemembership.DatabaseShard
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.QueryEvaluator
import org.wfanet.panelmatch.client.privatemembership.resultOf

/**
 * Fake [QueryEvaluator] for testing purposes.
 *
 * Each [EncryptedQueryBundle]'s payload is a serialized [ListValue] protocol buffer. Each element
 * in the list is a string -- the decimal string representation of a bucket to select.
 *
 * For example, a [ListValue] to select buckets 10 and 14 might be:
 *
 * values { string_value: "10" } values { string_value: "14" }
 *
 * No additional data is stored in the query metadata beyond the query id.
 */
object PlaintextQueryEvaluator : QueryEvaluator {
  override fun executeQueries(
    shards: List<DatabaseShard>,
    queryBundles: List<EncryptedQueryBundle>,
    serializedPublicKey: ByteString
  ): List<EncryptedQueryResult> {
    val results = mutableListOf<EncryptedQueryResult>()
    for (shard in shards) {
      for (bundle in queryBundles) {
        if (shard.shardId.id == bundle.shardId.id) {
          results.addAll(query(shard, bundle))
        }
      }
    }
    return results
  }

  private fun query(
    shard: DatabaseShard,
    bundle: EncryptedQueryBundle
  ): List<EncryptedQueryResult> {
    val queriedBuckets = ListValue.parseFrom(bundle.serializedEncryptedQueries)
    require(queriedBuckets.valuesCount == bundle.queryIdsCount)
    val results = mutableListOf<EncryptedQueryResult>()
    for ((queryId, queriedBucket) in bundle.queryIdsList zip queriedBuckets.valuesList) {
      val result =
        shard
          .bucketsList
          .filter { it.bucketId.id == queriedBucket.stringValue.toInt() }
          .map { resultOf(queryId, it.contents.toByteString()) }
          .ifEmpty { listOf(resultOf(queryId, ByteString.EMPTY)) }
          .single()
      results.add(result)
    }
    return results
  }
}
