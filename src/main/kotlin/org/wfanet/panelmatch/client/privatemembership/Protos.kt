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

package org.wfanet.panelmatch.client.privatemembership

import com.google.protobuf.ByteString

/** Constructs a [ShardId]. */
fun shardIdOf(id: Int): ShardId = shardId { this.id = id }

/** Constructs a [BucketId]. */
fun bucketIdOf(id: Int): BucketId = bucketId { this.id = id }

/** Constructs a [QueryId]. */
fun queryIdOf(id: Int): QueryId = queryId { this.id = id }

/** Constructs a [EncryptedEventData]. */
fun encryptedEventDataOf(ciphertexts: List<ByteString>): EncryptedEventData = encryptedEventData {
  this.ciphertexts += ciphertexts
}

/** Constructs a [UnencryptedQuery]. */
fun unencryptedQueryOf(shardId: ShardId, bucketId: BucketId, queryId: QueryId): UnencryptedQuery =
    unencryptedQuery {
  this.shardId = shardId
  this.bucketId = bucketId
  this.queryId = queryId
}

/** Constructs a [DecryptedQueryResult]. */
fun decryptedQueryOf(queryId: QueryId, bucketContents: Iterable<ByteString>): DecryptedQueryResult =
    decryptedQueryResult {
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
fun resultOf(queryId: QueryId, serializedEncryptedQueryResult: ByteString): EncryptedQueryResult =
    encryptedQueryResult {
  this.queryId = queryId
  this.serializedEncryptedQueryResult = serializedEncryptedQueryResult
}

/** Constructs a [EncryptedQueryBundle]. */
fun queryBundleOf(
  shardId: ShardId,
  queryIds: Iterable<QueryId>,
  serializedEncryptedQueries: ByteString
): EncryptedQueryBundle = encryptedQueryBundle {
  this.shardId = shardId
  this.queryIds += queryIds
  this.serializedEncryptedQueries = serializedEncryptedQueries
}

/** Constructs a [DatabaseKey]. */
fun databaseKeyOf(id: Long): DatabaseKey = databaseKey { this.id = id }

/** Constructs a [Plaintext]. */
fun plaintextOf(payload: ByteString): Plaintext = plaintext { this.payload = payload }

/** Constructs a [PanelistKey]. */
fun panelistKeyOf(id: Long): PanelistKey = panelistKey { this.id = id }

/** Constructs a [JoinKey]. */
fun joinKeyOf(key: ByteString): JoinKey = joinKey { this.key = key }
