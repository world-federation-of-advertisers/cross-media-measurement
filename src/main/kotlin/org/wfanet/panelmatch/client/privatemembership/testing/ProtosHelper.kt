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
import org.wfanet.panelmatch.client.privatemembership.DecryptedEventData
import org.wfanet.panelmatch.client.privatemembership.DecryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.EncryptedEventData
import org.wfanet.panelmatch.client.privatemembership.EncryptedQuery
import org.wfanet.panelmatch.client.privatemembership.UnencryptedQuery
import org.wfanet.panelmatch.client.privatemembership.bucketIdOf
import org.wfanet.panelmatch.client.privatemembership.decryptedEventData
import org.wfanet.panelmatch.client.privatemembership.decryptedQueryOf
import org.wfanet.panelmatch.client.privatemembership.encryptedEventDataOf
import org.wfanet.panelmatch.client.privatemembership.encryptedQueryOf
import org.wfanet.panelmatch.client.privatemembership.plaintextOf
import org.wfanet.panelmatch.client.privatemembership.queryIdOf
import org.wfanet.panelmatch.client.privatemembership.shardIdOf
import org.wfanet.panelmatch.client.privatemembership.unencryptedQueryOf
import org.wfanet.panelmatch.common.toByteString

/** Constructs a [UnencryptedQuery]. */
fun unencryptedQueryOf(shard: Int, query: Int, bucket: Int): UnencryptedQuery =
  unencryptedQueryOf(shardIdOf(shard), bucketIdOf(bucket), queryIdOf(query))

/** Constructs a [EncryptedQuery]. */
fun encryptedQueryOf(shard: Int, query: Int): EncryptedQuery =
  encryptedQueryOf(shardIdOf(shard), queryIdOf(query))

/** Constructs a [EncryptedEventData]. */
fun encryptedEventDataOf(ciphertext: ByteString, query: Int, shard: Int): EncryptedEventData {
  return encryptedEventDataOf(ciphertext, queryIdOf(query), shardIdOf(shard))
}

/** Constructs a [DecryptedEventData]. */
fun plaintextOf(plaintext: ByteString, query: Int, shard: Int): DecryptedEventData {
  return plaintextOf(plaintext, queryIdOf(query), shardIdOf(shard))
}

/** Constructs a [DecryptedEventData]. */
fun plaintextOf(plaintext: String, query: Int, shard: Int): DecryptedEventData {
  return plaintextOf(plaintext.toByteString(), query, shard)
}

/** Constructs a [DecryptedEventData]. */
fun plaintextOf(plaintext: String, query: Int): DecryptedEventData {
  return decryptedEventData {
    this.plaintext = plaintext.toByteString()
    queryId = queryIdOf(query)
  }
}

/** Constructs a [DecryptedQueryResult]. */
fun decryptedQueryOf(queryResult: ByteString, query: Int, shard: Int): DecryptedQueryResult {
  return decryptedQueryOf(queryResult, queryIdOf(query), shardIdOf(shard))
}
