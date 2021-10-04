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

import com.google.privatemembership.batch.Shared.EncryptedQueryResult as ClientEncryptedQueryResult
import com.google.privatemembership.batch.client.decryptQueriesRequest
import com.google.privatemembership.batch.client.encryptQueriesRequest
import com.google.privatemembership.batch.client.plaintextQuery
import com.google.privatemembership.batch.queryMetadata
import com.google.protobuf.ByteString
import org.wfanet.panelmatch.client.privatemembership.BucketId
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.JniPrivateMembership
import org.wfanet.panelmatch.client.privatemembership.JniQueryEvaluator
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.ShardId
import org.wfanet.panelmatch.client.privatemembership.bucketIdOf
import org.wfanet.panelmatch.client.privatemembership.bucketOf
import org.wfanet.panelmatch.client.privatemembership.databaseShardOf
import org.wfanet.panelmatch.client.privatemembership.shardIdOf
import org.wfanet.panelmatch.common.toByteString

class JniQueryEvaluatorTestHelper(private val context: JniQueryEvaluatorContext) :
  QueryEvaluatorTestHelper {
  override fun decodeResultData(result: EncryptedQueryResult): ByteString {
    val encryptedQueryResult =
      ClientEncryptedQueryResult.parseFrom(result.serializedEncryptedQueryResult)
    if (encryptedQueryResult.ciphertextsCount == 0) {
      // TODO(@efoxepstein): why is this required?
      return ByteString.EMPTY
    }
    val request = decryptQueriesRequest {
      parameters = context.privateMembershipParameters
      publicKey = context.privateMembershipPublicKey
      privateKey = context.privateMembershipPrivateKey
      encryptedQueries += encryptedQueryResult
    }
    val response = JniPrivateMembership.decryptQueries(request)
    return response.resultList.single().result
  }

  override fun makeQueryBundle(
    shard: ShardId,
    queries: List<Pair<QueryId, BucketId>>
  ): EncryptedQueryBundle {
    val request = encryptQueriesRequest {
      parameters = context.privateMembershipParameters
      publicKey = context.privateMembershipPublicKey
      privateKey = context.privateMembershipPrivateKey
      plaintextQueries +=
        queries.map { (queryId, bucketId) ->
          plaintextQuery {
            this.bucketId = bucketId.id
            queryMetadata =
              queryMetadata {
                shardId = shard.id
                this.queryId = queryId.id
              }
          }
        }
    }
    val response = JniPrivateMembership.encryptQueries(request)
    return org.wfanet.panelmatch.client.privatemembership.queryBundleOf(
      shard,
      queries.map { it.first },
      response.encryptedQueries.toByteString()
    )
  }

  override fun makeResult(query: QueryId, rawPayload: ByteString): EncryptedQueryResult {
    // TODO(@efoxepstein): have private-membership expose a helper for this.
    return JniQueryEvaluator(context.privateMembershipParameters.toByteString())
      .executeQueries(
        listOf(databaseShardOf(shardIdOf(0), listOf(bucketOf(bucketIdOf(0), rawPayload)))),
        listOf(makeQueryBundle(shardIdOf(0), listOf(query to bucketIdOf(0)))),
        serializedPublicKey
      )
      .single()
  }

  override fun makeEmptyResult(query: QueryId): EncryptedQueryResult {
    // TODO(@efoxepstein): have private-membership expose a helper for this.
    val databaseShard =
      databaseShardOf(
        shardIdOf(0),
        listOf(bucketOf(bucketIdOf(1), "some-unused-payload".toByteString()))
      )
    val queryBundle = makeQueryBundle(shardIdOf(0), listOf(query to bucketIdOf(0)))

    return JniQueryEvaluator(context.privateMembershipParameters.toByteString())
      .executeQueries(listOf(databaseShard), listOf(queryBundle), serializedPublicKey)
      .single()
  }

  override val serializedPublicKey: ByteString = context.privateMembershipPublicKey.toByteString()
}
