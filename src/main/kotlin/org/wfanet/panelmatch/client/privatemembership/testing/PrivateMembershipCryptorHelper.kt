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

import java.io.Serializable
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.privatemembership.BucketId
import org.wfanet.panelmatch.client.privatemembership.DecryptedEventData
import org.wfanet.panelmatch.client.privatemembership.EncryptedEventData
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.PanelistKey
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipDecryptRequest
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipDecryptResponse
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipEncryptResponse
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.ShardId
import org.wfanet.panelmatch.client.privatemembership.bucketIdOf
import org.wfanet.panelmatch.client.privatemembership.panelistKeyOf
import org.wfanet.panelmatch.client.privatemembership.queryIdOf
import org.wfanet.panelmatch.client.privatemembership.shardIdOf

/** Used for testing CreateQueriesWorkflow (eg reversing some of the operations) */
interface PrivateMembershipCryptorHelper : Serializable {

  /**
   * Takes a list of pairs of [EncryptedEventData] and returns an encrypted list of
   * [EncryptedQueryResult]
   */
  fun makeEncryptedQueryResults(
    encryptedEventData: List<EncryptedEventData>
  ): List<EncryptedQueryResult>

  /**
   * Takes a list [DecryptedEventData] and a list of pairs of (QueryId, ByteString) and returns an
   * encrypted list of [EncryptedEventData]
   */
  fun makeEncryptedEventData(
    plaintexts: List<DecryptedEventData>,
    joinkeys: List<Pair<Int, String>>
  ): List<EncryptedEventData>

  /** Used for testing. Removes a single layer of encryption from encrypted query results. */
  fun decryptQueryResults(
    request: PrivateMembershipDecryptRequest
  ): PrivateMembershipDecryptResponse

  /**
   * Takes an [PrivateMembershipEncryptResponse] and reverses the process to yield the underlying
   * decrypted [ShardedQuery] for each [QueryId]
   */
  fun decodeEncryptedQuery(
    data: PCollection<PrivateMembershipEncryptResponse>
  ): PCollection<KV<QueryId, ShardedQuery>>
}

data class ShardedQuery(val shardId: ShardId, val queryId: QueryId, val bucketId: BucketId) :
  Serializable {
  constructor(
    shard: Int,
    query: Int,
    bucket: Int
  ) : this(shardIdOf(shard), queryIdOf(query), bucketIdOf(bucket))
}

data class PanelistQuery(
  val shardId: ShardId,
  val panelistKey: PanelistKey,
  val bucketId: BucketId
) : Serializable {
  constructor(
    shard: Int,
    panelist: Long,
    bucket: Int
  ) : this(shardIdOf(shard), panelistKeyOf(panelist), bucketIdOf(bucket))
}
