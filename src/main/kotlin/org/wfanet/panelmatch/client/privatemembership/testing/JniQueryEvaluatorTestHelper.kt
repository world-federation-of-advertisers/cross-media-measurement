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
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import org.wfanet.panelmatch.client.common.bucketIdOf
import org.wfanet.panelmatch.client.common.bucketOf
import org.wfanet.panelmatch.client.common.databaseShardOf
import org.wfanet.panelmatch.client.common.encryptedQueryBundleOf
import org.wfanet.panelmatch.client.common.shardIdOf
import org.wfanet.panelmatch.client.privatemembership.BucketContents
import org.wfanet.panelmatch.client.privatemembership.BucketId
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.JniPrivateMembership
import org.wfanet.panelmatch.client.privatemembership.JniQueryEvaluator
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.ShardId
import org.wfanet.panelmatch.client.privatemembership.bucketContents
import org.wfanet.panelmatch.client.privatemembership.paddingNonce

class JniQueryEvaluatorTestHelper(private val context: JniQueryEvaluatorContext) :
  QueryEvaluatorTestHelper {
  override fun decodeResultData(result: EncryptedQueryResult): BucketContents {
    val encryptedQueryResult =
      ClientEncryptedQueryResult.parseFrom(result.serializedEncryptedQueryResult)
    if (encryptedQueryResult.ciphertextsCount == 0) {
      // TODO(@efoxepstein): why is this required?
      return bucketContents {}
    }
    val request = decryptQueriesRequest {
      parameters = context.privateMembershipParameters
      publicKey = context.privateMembershipPublicKey
      privateKey = context.privateMembershipPrivateKey
      encryptedQueries += encryptedQueryResult
    }
    val response = JniPrivateMembership.decryptQueries(request)
    return BucketContents.parseFrom(response.resultList.single().result)
  }

  override fun makeQueryBundle(
    shard: ShardId,
    queries: List<Pair<QueryId, BucketId>>,
  ): EncryptedQueryBundle {
    val request = encryptQueriesRequest {
      parameters = context.privateMembershipParameters
      publicKey = context.privateMembershipPublicKey
      privateKey = context.privateMembershipPrivateKey
      plaintextQueries +=
        queries.map { (queryId, bucketId) ->
          plaintextQuery {
            this.bucketId = bucketId.id
            queryMetadata = queryMetadata {
              shardId = shard.id
              this.queryId = queryId.id
            }
          }
        }
    }
    val response = JniPrivateMembership.encryptQueries(request)
    return encryptedQueryBundleOf(
      shard,
      queries.map { it.first },
      response.encryptedQueries.toByteString(),
    )
  }

  override fun makeResult(query: QueryId, rawPayload: ByteString): EncryptedQueryResult {
    // TODO(@efoxepstein): have private-membership expose a helper for this.
    return JniQueryEvaluator(Any.pack(context.privateMembershipParameters))
      .executeQueries(
        listOf(databaseShardOf(shardIdOf(0), listOf(bucketOf(bucketIdOf(0), listOf(rawPayload))))),
        listOf(makeQueryBundle(shardIdOf(0), listOf(query to bucketIdOf(0)))),
        mapOf(query to paddingNonce {}),
        serializedPublicKey,
      )
      .single()
  }

  override fun makeEmptyResult(query: QueryId): EncryptedQueryResult {
    // TODO(@efoxepstein): have private-membership expose a helper for this.
    val databaseShard =
      databaseShardOf(
        shardIdOf(0),
        listOf(bucketOf(bucketIdOf(1), listOf("some-unused-payload".toByteStringUtf8()))),
      )
    val queryBundle = makeQueryBundle(shardIdOf(0), listOf(query to bucketIdOf(0)))
    val paddingNonces = mapOf(query to paddingNonce {})

    return JniQueryEvaluator(Any.pack(context.privateMembershipParameters))
      .executeQueries(
        listOf(databaseShard),
        listOf(queryBundle),
        paddingNonces,
        serializedPublicKey,
      )
      .single()
  }

  override val serializedPublicKey: ByteString = context.privateMembershipPublicKey.toByteString()
}
