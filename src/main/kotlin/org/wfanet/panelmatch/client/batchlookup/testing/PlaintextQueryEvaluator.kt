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

import com.google.protobuf.ByteString
import com.google.protobuf.ListValue
import org.wfanet.panelmatch.client.batchlookup.DatabaseShard
import org.wfanet.panelmatch.client.batchlookup.QueryBundle
import org.wfanet.panelmatch.client.batchlookup.QueryEvaluator
import org.wfanet.panelmatch.client.batchlookup.Result
import org.wfanet.panelmatch.client.batchlookup.resultOf

/**
 * Fake [QueryEvaluator] for testing purposes.
 *
 * Each [QueryBundle]'s payload is a serialized [ListValue] protocol buffer. Each element in the
 * list is a string -- the decimal string representation of a bucket to select.
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
    queryBundles: List<QueryBundle>
  ): List<Result> {
    val results = mutableListOf<Result>()
    for (shard in shards) {
      for (bundle in queryBundles) {
        if (shard.shardId.id == bundle.shardId.id) {
          results.addAll(query(shard, bundle))
        }
      }
    }
    return results
  }

  override fun combineResults(results: Sequence<Result>): Result {
    val resultsList = results.toList()
    require(resultsList.isNotEmpty())

    val queryId = resultsList.first().queryMetadata.queryId.id

    for (result in resultsList) {
      require(result.queryMetadata.queryId.id == queryId)
    }

    val nonEmptyResults = resultsList.filter { !it.payload.isEmpty }

    return when (nonEmptyResults.size) {
      0 -> resultsList.first()
      else -> nonEmptyResults.first()
    }
  }

  private fun query(shard: DatabaseShard, bundle: QueryBundle): List<Result> {
    val queriedBuckets = ListValue.parseFrom(bundle.payload)
    require(queriedBuckets.valuesCount == bundle.queryMetadataList.size)
    val results = mutableListOf<Result>()
    for ((metadata, queriedBucket) in bundle.queryMetadataList zip queriedBuckets.valuesList) {
      val result =
        shard
          .bucketsList
          .filter { it.bucketId.id == queriedBucket.stringValue.toInt() }
          .map { resultOf(metadata, it.payload) }
          .ifEmpty { listOf(resultOf(metadata, ByteString.EMPTY)) }
          .single()
      results.add(result)
    }
    return results
  }
}
