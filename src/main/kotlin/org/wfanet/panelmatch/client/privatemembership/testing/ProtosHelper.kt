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
import org.wfanet.panelmatch.client.common.bucketIdOf
import org.wfanet.panelmatch.client.common.decryptedQueryOf
import org.wfanet.panelmatch.client.common.joinKeyIdentifierOf
import org.wfanet.panelmatch.client.common.joinKeyOf
import org.wfanet.panelmatch.client.common.plaintextOf
import org.wfanet.panelmatch.client.common.queryIdOf
import org.wfanet.panelmatch.client.common.shardIdOf
import org.wfanet.panelmatch.client.common.unencryptedQueryOf
import org.wfanet.panelmatch.client.exchangetasks.JoinKey
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyIdentifier
import org.wfanet.panelmatch.client.exchangetasks.joinKey
import org.wfanet.panelmatch.client.privatemembership.DecryptEventDataRequest.EncryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.DecryptEventDataRequestKt.encryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.DecryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.Plaintext
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.QueryIdAndJoinKeys
import org.wfanet.panelmatch.client.privatemembership.ShardId
import org.wfanet.panelmatch.client.privatemembership.UnencryptedQuery
import org.wfanet.panelmatch.client.privatemembership.encryptedEventData
import org.wfanet.panelmatch.client.privatemembership.queryIdAndJoinKeys

/** Constructs a [UnencryptedQuery]. */
fun unencryptedQueryOf(shard: Int, query: Int, bucket: Int): UnencryptedQuery =
  unencryptedQueryOf(shardIdOf(shard), bucketIdOf(bucket), queryIdOf(query))

data class EncryptedQuery(val shard: ShardId, val query: QueryId)

/** Constructs a [EncryptedQuery]. */
fun encryptedQueryOf(shard: Int, query: Int): EncryptedQuery =
  EncryptedQuery(shardIdOf(shard), queryIdOf(query))

/** Constructs a [EncryptedEventDataSet]. */
fun encryptedEventDataSetOf(ciphertexts: List<String>, query: Int): EncryptedEventDataSet =
    encryptedEventDataSet {
  queryId = queryIdOf(query)
  this.encryptedEventData =
    encryptedEventData { this.ciphertexts += ciphertexts.map { it.toByteStringUtf8() } }
}

/** Constructs a [DecryptedQueryResult]. */
fun decryptedQueryOf(query: Int, queryResult: ByteString): DecryptedQueryResult =
  decryptedQueryOf(queryIdOf(query), listOf(queryResult))

fun plaintextOf(payload: String): Plaintext = plaintextOf(payload.toByteStringUtf8())

fun joinKeyOf(key: String): JoinKey = joinKeyOf(key.toByteStringUtf8())

fun joinKeyOf(key: Long): JoinKey = joinKeyOf("joinKey of $key")

fun joinKeyIdentifierOf(key: Long): JoinKeyIdentifier =
  joinKeyIdentifierOf("joinKeyIdentifier of $key".toByteStringUtf8())

fun queryIdAndJoinKeysOf(query: Int, lookup: String, join: String): QueryIdAndJoinKeys =
    queryIdAndJoinKeys {
  queryId = queryIdOf(query)
  lookupKey = joinKey { key = lookup.toByteStringUtf8() }
  hashedJoinKey = joinKey { key = join.toByteStringUtf8() }
}
