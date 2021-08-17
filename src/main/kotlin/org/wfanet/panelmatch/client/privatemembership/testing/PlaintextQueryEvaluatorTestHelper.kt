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
import org.wfanet.panelmatch.client.privatemembership.BucketId
import org.wfanet.panelmatch.client.privatemembership.QueryBundle
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.Result
import org.wfanet.panelmatch.client.privatemembership.ShardId
import org.wfanet.panelmatch.client.privatemembership.queryBundleOf
import org.wfanet.panelmatch.client.privatemembership.queryMetadataOf
import org.wfanet.panelmatch.client.privatemembership.resultOf

/**
 * Helper with [PlaintextQueryEvaluator].
 *
 * See documentation for [PlaintextQueryEvaluator] for details on the internal format of the
 * [QueryBundle] payload.
 */
object PlaintextQueryEvaluatorTestHelper : QueryEvaluatorTestHelper {
  override fun decodeResultData(result: Result): ByteString {
    return result.payload
  }

  override fun makeQueryBundle(
    shard: ShardId,
    queries: List<Pair<QueryId, BucketId>>
  ): QueryBundle {
    return queryBundleOf(
      shard,
      queries.map { queryMetadataOf(it.first, ByteString.EMPTY) },
      ListValue.newBuilder()
        .apply {
          for (query in queries) {
            addValuesBuilder().stringValue = query.second.id.toString()
          }
        }
        .build()
        .toByteString()
    )
  }

  override fun makeResult(query: QueryId, rawPayload: ByteString): Result {
    return resultOf(queryMetadataOf(query, ByteString.EMPTY), rawPayload)
  }
}
