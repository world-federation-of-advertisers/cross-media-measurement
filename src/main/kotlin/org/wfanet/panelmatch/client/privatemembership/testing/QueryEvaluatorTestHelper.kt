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
import java.io.Serializable
import org.wfanet.panelmatch.client.privatemembership.BucketContents
import org.wfanet.panelmatch.client.privatemembership.BucketId
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.ShardId

interface QueryEvaluatorTestHelper : Serializable {
  data class DecodedResult(val queryId: Int, val data: BucketContents) : Serializable {
    override fun toString(): String {
      return "DecodedResult(query=$queryId, data=$data)"
    }
  }

  fun decodeResult(result: EncryptedQueryResult): DecodedResult {
    return DecodedResult(result.queryId.id, decodeResultData(result))
  }

  fun decodeResultData(result: EncryptedQueryResult): BucketContents

  fun makeQueryBundle(shard: ShardId, queries: List<Pair<QueryId, BucketId>>): EncryptedQueryBundle

  fun makeResult(query: QueryId, rawPayload: ByteString): EncryptedQueryResult

  fun makeEmptyResult(query: QueryId): EncryptedQueryResult

  val serializedPublicKey: ByteString
}
