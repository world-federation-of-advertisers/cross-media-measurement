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
import org.wfanet.panelmatch.client.common.bucketIdOf
import org.wfanet.panelmatch.client.common.queryIdOf
import org.wfanet.panelmatch.client.common.shardIdOf
import org.wfanet.panelmatch.client.exchangetasks.JoinKey
import org.wfanet.panelmatch.client.privatemembership.BucketId
import org.wfanet.panelmatch.client.privatemembership.DecryptEventDataRequest.EncryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.DecryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.DecryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.ShardId
import org.wfanet.panelmatch.common.crypto.AsymmetricKeyPair

/** Helps test the Private Membership implementation (e.g. by reversing some operations) */
interface PrivateMembershipCryptorHelper : Serializable {

  /** Constructs an encrypted query bundle */
  fun makeEncryptedQueryBundle(
    shard: ShardId,
    queries: List<Pair<QueryId, BucketId>>,
  ): EncryptedQueryBundle

  /** Decodes an encrypted query bundle */
  fun decodeEncryptedQueryBundle(queryBundle: EncryptedQueryBundle): List<ShardedQuery>

  /** Constructs an [EncryptedQueryResult] from an [EncryptedEventDataSet] */
  fun makeEncryptedQueryResult(
    keys: AsymmetricKeyPair,
    encryptedEventDataSet: EncryptedEventDataSet,
  ): EncryptedQueryResult

  /** Decodes an encrypted query result */
  fun decodeEncryptedQueryResult(result: EncryptedQueryResult): DecryptedQueryResult

  /**
   * Takes a [DecryptedEventDataSet] and a list of pairs of (QueryId, JoinKey) and returns a
   * [EncryptedEventDataSet]
   */
  fun makeEncryptedEventDataSet(
    plaintext: DecryptedEventDataSet,
    joinkey: Pair<QueryId, JoinKey>,
  ): EncryptedEventDataSet
}

data class ShardedQuery(val shardId: ShardId, val queryId: QueryId, val bucketId: BucketId) :
  Serializable {
  constructor(
    shard: Int,
    query: Int,
    bucket: Int,
  ) : this(shardIdOf(shard), queryIdOf(query), bucketIdOf(bucket))
}
