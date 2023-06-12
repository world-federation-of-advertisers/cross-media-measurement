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

import com.google.protobuf.ListValue
import com.google.protobuf.listValue
import com.google.protobuf.value
import org.wfanet.panelmatch.client.privatemembership.BucketId
import org.wfanet.panelmatch.client.privatemembership.DecryptEventDataRequest.EncryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.DecryptEventDataRequestKt.encryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.DecryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.DecryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.JoinKey
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipKeys
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.ShardId
import org.wfanet.panelmatch.client.privatemembership.decryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.encryptedEventData
import org.wfanet.panelmatch.client.privatemembership.queryBundleOf
import org.wfanet.panelmatch.client.privatemembership.resultOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.values
import org.wfanet.panelmatch.common.crypto.SymmetricCryptor
import org.wfanet.panelmatch.common.crypto.testing.FakeSymmetricCryptor
import org.wfanet.panelmatch.common.toByteString

class PlaintextPrivateMembershipCryptorHelper : PrivateMembershipCryptorHelper {

  private val symmetricCryptor: SymmetricCryptor = FakeSymmetricCryptor()

  override fun makeEncryptedQueryBundle(
    shard: ShardId,
    queries: List<Pair<QueryId, BucketId>>
  ): EncryptedQueryBundle {
    return queryBundleOf(
      shard,
      queries.map { it.first },
      listValue {
          for (query in queries) {
            values += value { stringValue = query.second.id.toString() }
          }
        }
        .toByteString()
    )
  }

  override fun decodeEncryptedQueryBundle(queryBundle: EncryptedQueryBundle): List<ShardedQuery> {
    val queryIdsList = queryBundle.queryIdsList
    val bucketValuesList =
      ListValue.parseFrom(queryBundle.serializedEncryptedQueries).valuesList.map {
        it.stringValue.toInt()
      }
    return queryIdsList.zip(bucketValuesList) { queryId: QueryId, bucketValue: Int ->
      ShardedQuery(requireNotNull(queryBundle.shardId).id, queryId.id, bucketValue)
    }
  }

  override fun makeEncryptedQueryResult(
    keys: PrivateMembershipKeys,
    encryptedEventDataSet: EncryptedEventDataSet
  ): EncryptedQueryResult {
    return resultOf(
      encryptedEventDataSet.queryId,
      encryptedEventDataSet.encryptedEventData.toByteString()
    )
  }

  override fun decodeEncryptedQueryResult(result: EncryptedQueryResult): DecryptedQueryResult {
    return decryptedQueryResult {
      queryId = result.queryId
      queryResult = result.serializedEncryptedQueryResult
    }
  }

  override fun makeEncryptedEventDataSet(
    plaintext: DecryptedEventDataSet,
    joinkey: Pair<QueryId, JoinKey>
  ): EncryptedEventDataSet {
    require(plaintext.queryId == joinkey.first) { "QueryId must be the same" }
    return encryptedEventDataSet {
      queryId = plaintext.queryId
      encryptedEventData =
        encryptedEventData {
          ciphertexts +=
            plaintext.decryptedEventDataList.map {
              symmetricCryptor.encrypt(joinkey.second.key, it.payload)
            }
        }
    }
  }
}
