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

import com.google.privatemembership.batch.Shared.EncryptedQueries
import com.google.privatemembership.batch.Shared.Parameters
import com.google.privatemembership.batch.Shared.PublicKey
import com.google.privatemembership.batch.server.ApplyQueriesRequestKt.rawDatabase
import com.google.privatemembership.batch.server.RawDatabaseShardKt.bucket
import com.google.privatemembership.batch.server.Server.RawDatabaseShard
import com.google.privatemembership.batch.server.applyQueriesRequest
import com.google.privatemembership.batch.server.rawDatabaseShard
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import org.wfanet.panelmatch.client.common.encryptedQueryResultOf
import org.wfanet.panelmatch.client.common.queryIdOf

/** [QueryEvaluator] that calls into C++ via JNI. */
class JniQueryEvaluator(parameters: Any) : QueryEvaluator {

  private val privateMembershipParameters by lazy { parameters.unpack(Parameters::class.java) }

  init {
    check(privateMembershipParameters.shardParameters.enablePaddingNonces) {
      "Padding Nonces are required"
    }
  }

  override fun executeQueries(
    shards: List<DatabaseShard>,
    queryBundles: List<EncryptedQueryBundle>,
    paddingNonces: Map<QueryId, PaddingNonce>,
    serializedPublicKey: ByteString,
  ): List<EncryptedQueryResult> {
    val presentDatabaseShards = shards.map { it.shardId.id }.toSet()
    val presentQueryShards = queryBundles.map { it.shardId.id }.toSet()
    require(presentDatabaseShards == presentQueryShards) {
      "Mismatching shards: $presentDatabaseShards vs $presentQueryShards"
    }

    val request = applyQueriesRequest {
      parameters = privateMembershipParameters
      publicKey = PublicKey.parseFrom(serializedPublicKey)
      finalizeResults = true

      for (bundle in queryBundles) {
        queries += bundle.encryptedQueries
        for (queryId in bundle.queryIdsList) {
          this.paddingNonces[queryId.id] = paddingNonces.getValue(queryId).nonce
        }
      }

      rawDatabase = rawDatabase {
        this.shards += shards.map { it.toPrivateMembershipRawDatabaseShard() }
      }
    }

    check(request.queriesList.all { !it.prngSeed.isEmpty })

    val response = JniPrivateMembership.applyQueries(request)

    val inputQueryCount = queryBundles.sumOf { it.queryIdsCount }
    require(response.queryResultsCount == inputQueryCount) {
      "Output query count (${response.queryResultsCount}) is not the same as the input query " +
        "count ($inputQueryCount)"
    }

    return response.queryResultsList.map { encryptedQueryResult ->
      encryptedQueryResultOf(
        queryIdOf(encryptedQueryResult.queryMetadata.queryId),
        encryptedQueryResult.toByteString(),
      )
    }
  }
}

private val EncryptedQueryBundle.encryptedQueries: EncryptedQueries
  get() = EncryptedQueries.parseFrom(serializedEncryptedQueries)

private fun DatabaseShard.toPrivateMembershipRawDatabaseShard(): RawDatabaseShard {
  return rawDatabaseShard {
    shardIndex = shardId.id
    buckets +=
      bucketsList.map {
        bucket {
          bucketId = it.bucketId.id
          bucketContents = it.contents.toByteString()
        }
      }
  }
}
