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
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.protobuf.listValue
import com.google.protobuf.value
import org.wfanet.panelmatch.client.common.encryptedQueryBundleOf
import org.wfanet.panelmatch.client.common.encryptedQueryResultOf
import org.wfanet.panelmatch.client.privatemembership.BucketContents
import org.wfanet.panelmatch.client.privatemembership.BucketId
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.ShardId

/**
 * Helper with [PlaintextQueryEvaluator].
 *
 * See documentation for [PlaintextQueryEvaluator] for details on the internal format of the
 * [EncryptedQueryBundle] payload.
 */
object PlaintextQueryEvaluatorTestHelper : QueryEvaluatorTestHelper {
  override fun decodeResultData(result: EncryptedQueryResult): BucketContents {
    return BucketContents.parseFrom(result.serializedEncryptedQueryResult)
  }

  override fun makeQueryBundle(
    shard: ShardId,
    queries: List<Pair<QueryId, BucketId>>,
  ): EncryptedQueryBundle {
    return encryptedQueryBundleOf(
      shard,
      queries.map { it.first },
      listValue {
          for (query in queries) {
            values += value { stringValue = query.second.id.toString() }
          }
        }
        .toByteString(),
    )
  }

  override fun makeResult(query: QueryId, rawPayload: ByteString): EncryptedQueryResult {
    return encryptedQueryResultOf(query, rawPayload)
  }

  override fun makeEmptyResult(query: QueryId): EncryptedQueryResult {
    return makeResult(query, ByteString.EMPTY)
  }

  override val serializedPublicKey: ByteString = "some-serialized-public-key".toByteStringUtf8()
}
