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

package org.wfanet.panelmatch.client.common

import com.google.protobuf.ByteString
import org.wfanet.panelmatch.client.eventpreprocessing.UnprocessedEvent
import org.wfanet.panelmatch.client.eventpreprocessing.unprocessedEvent
import org.wfanet.panelmatch.client.exchangetasks.JoinKey
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyAndId
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyAndIdCollection
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyIdentifier
import org.wfanet.panelmatch.client.exchangetasks.joinKey
import org.wfanet.panelmatch.client.exchangetasks.joinKeyAndId
import org.wfanet.panelmatch.client.exchangetasks.joinKeyAndIdCollection
import org.wfanet.panelmatch.client.exchangetasks.joinKeyIdentifier
import org.wfanet.panelmatch.client.privatemembership.Bucket
import org.wfanet.panelmatch.client.privatemembership.BucketId
import org.wfanet.panelmatch.client.privatemembership.DatabaseEntry
import org.wfanet.panelmatch.client.privatemembership.DatabaseShard
import org.wfanet.panelmatch.client.privatemembership.DecryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.EncryptedEntry
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.LookupKey
import org.wfanet.panelmatch.client.privatemembership.LookupKeyAndId
import org.wfanet.panelmatch.client.privatemembership.PaddingNonce
import org.wfanet.panelmatch.client.privatemembership.Plaintext
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.ShardId
import org.wfanet.panelmatch.client.privatemembership.UnencryptedQuery
import org.wfanet.panelmatch.client.privatemembership.bucket
import org.wfanet.panelmatch.client.privatemembership.bucketContents
import org.wfanet.panelmatch.client.privatemembership.bucketId
import org.wfanet.panelmatch.client.privatemembership.databaseEntry
import org.wfanet.panelmatch.client.privatemembership.databaseShard
import org.wfanet.panelmatch.client.privatemembership.decryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.encryptedEntry
import org.wfanet.panelmatch.client.privatemembership.encryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.encryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.lookupKey
import org.wfanet.panelmatch.client.privatemembership.lookupKeyAndId
import org.wfanet.panelmatch.client.privatemembership.paddingNonce
import org.wfanet.panelmatch.client.privatemembership.plaintext
import org.wfanet.panelmatch.client.privatemembership.queryId
import org.wfanet.panelmatch.client.privatemembership.shardId
import org.wfanet.panelmatch.client.privatemembership.unencryptedQuery

/** Constructs a [ShardId]. */
fun shardIdOf(id: Int): ShardId = shardId { this.id = id }

/** Constructs a [BucketId]. */
fun bucketIdOf(id: Int): BucketId = bucketId { this.id = id }

/** Constructs a [QueryId]. */
fun queryIdOf(id: Int): QueryId = queryId { this.id = id }

/** Constructs a [UnencryptedQuery]. */
fun unencryptedQueryOf(shardId: ShardId, bucketId: BucketId, queryId: QueryId): UnencryptedQuery =
  unencryptedQuery {
    this.shardId = shardId
    this.bucketId = bucketId
    this.queryId = queryId
  }

/** Constructs a [DecryptedQueryResult]. */
fun decryptedQueryResultOf(
  queryId: QueryId,
  bucketContents: Iterable<ByteString>,
): DecryptedQueryResult = decryptedQueryResult {
  this.queryId = queryId
  this.queryResult = bucketContents { items += bucketContents }
}

/** Constructs a [DatabaseShard]. */
fun databaseShardOf(shardId: ShardId, buckets: Iterable<Bucket>): DatabaseShard = databaseShard {
  this.shardId = shardId
  this.buckets += buckets
}

/** Constructs a [Bucket]. */
fun bucketOf(bucketId: BucketId, items: Iterable<ByteString>): Bucket = bucket {
  this.bucketId = bucketId
  this.contents = bucketContents { this.items += items }
}

/** Constructs a [Result]. */
fun encryptedQueryResultOf(
  queryId: QueryId,
  serializedEncryptedQueryResult: ByteString,
): EncryptedQueryResult = encryptedQueryResult {
  this.queryId = queryId
  this.serializedEncryptedQueryResult = serializedEncryptedQueryResult
}

/** Constructs a [EncryptedQueryBundle]. */
fun encryptedQueryBundleOf(
  shardId: ShardId,
  queryIds: Iterable<QueryId>,
  serializedEncryptedQueries: ByteString,
): EncryptedQueryBundle = encryptedQueryBundle {
  this.shardId = shardId
  this.queryIds += queryIds
  this.serializedEncryptedQueries = serializedEncryptedQueries
}

/** Constructs a [Plaintext]. */
fun plaintextOf(payload: ByteString): Plaintext = plaintext { this.payload = payload }

/** Constructs a [JoinKey]. */
fun joinKeyOf(key: ByteString): JoinKey = joinKey { this.key = key }

/** Constructs a [JoinKeyIdentifier]. */
fun joinKeyIdentifierOf(id: ByteString): JoinKeyIdentifier = joinKeyIdentifier { this.id = id }

/** Constructs a [JoinKeyAndId]. */
fun joinKeyAndIdOf(key: ByteString, id: ByteString): JoinKeyAndId = joinKeyAndId {
  joinKey = joinKeyOf(key)
  joinKeyIdentifier = joinKeyIdentifierOf(id)
}

/** Constructs a [JoinKeyAndIdCollection]. */
fun joinKeyAndIdCollectionOf(items: List<JoinKeyAndId>): JoinKeyAndIdCollection =
  joinKeyAndIdCollection {
    joinKeyAndIds += items
  }

/** Constructs a [EncryptedEntry]. */
fun encryptedEntryOf(data: ByteString): EncryptedEntry = encryptedEntry { this.data = data }

/** Constructs a [DatabaseEntry]. */
fun databaseEntryOf(lookupKey: LookupKey, encryptedEntry: EncryptedEntry): DatabaseEntry =
  databaseEntry {
    this.lookupKey = lookupKey
    this.encryptedEntry = encryptedEntry
  }

/** Constructs a [UnprocessedEvent]. */
fun unprocessedEventOf(eventId: ByteString, eventData: ByteString): UnprocessedEvent =
  unprocessedEvent {
    id = eventId
    data = eventData
  }

/** Constructs a [LookupKey]. */
fun lookupKeyOf(key: Long): LookupKey = lookupKey { this.key = key }

/** Constructs a [LookupKeyAndId]. */
fun lookupKeyAndIdOf(key: Long, id: ByteString): LookupKeyAndId = lookupKeyAndId {
  lookupKey = lookupKeyOf(key)
  joinKeyIdentifier = joinKeyIdentifierOf(id)
}

/** Constructs a [PaddingNonce]. */
fun paddingNonceOf(nonce: ByteString): PaddingNonce = paddingNonce { this.nonce = nonce }
