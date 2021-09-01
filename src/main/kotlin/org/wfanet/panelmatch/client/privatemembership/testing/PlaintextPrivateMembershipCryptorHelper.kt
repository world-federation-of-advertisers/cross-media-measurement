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
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.privatemembership.DecryptedEventData
import org.wfanet.panelmatch.client.privatemembership.EncryptQueriesResponse
import org.wfanet.panelmatch.client.privatemembership.EncryptedEventData
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.QueryBundle
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.QueryMetadata
import org.wfanet.panelmatch.client.privatemembership.bucketIdOf
import org.wfanet.panelmatch.client.privatemembership.encryptedEventData
import org.wfanet.panelmatch.client.privatemembership.encryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.queryIdOf
import org.wfanet.panelmatch.client.privatemembership.queryMetadataOf
import org.wfanet.panelmatch.client.privatemembership.resultOf
import org.wfanet.panelmatch.client.privatemembership.shardIdOf
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.parDo
import org.wfanet.panelmatch.common.crypto.SymmetricCryptor
import org.wfanet.panelmatch.common.crypto.testing.ConcatSymmetricCryptor
import org.wfanet.panelmatch.common.toByteString

object PlaintextPrivateMembershipCryptorHelper : PrivateMembershipCryptorHelper {

  private val symmetricCryptor: SymmetricCryptor = ConcatSymmetricCryptor()

  private fun queryBundleOf(shard: Int, queries: List<Pair<Int, Int>>): QueryBundle {
    return PlaintextQueryEvaluatorTestHelper.makeQueryBundle(
      shardIdOf(shard),
      queries.map { queryIdOf(it.first) to bucketIdOf(it.second) }
    )
  }

  private fun decodeQueryBundle(queryBundle: QueryBundle): List<ShardedQuery> {
    val queryBundleList = queryBundle.queryMetadataList
    val bucketValuesList =
      ListValue.parseFrom(queryBundle.payload).valuesList.map { it.stringValue.toInt() }
    return queryBundleList.zip(bucketValuesList) { a: QueryMetadata, b: Int ->
      ShardedQuery(requireNotNull(queryBundle.shardId).id, a.queryId.id, b)
    }
  }

  override fun makeEncryptedQueryResults(
    encryptedEventData: List<EncryptedEventData>
  ): List<EncryptedQueryResult> {
    return encryptedEventData.map {
      encryptedQueryResult {
        queryId = it.queryId
        shardId = it.shardId
        ciphertexts +=
          resultOf(queryMetadataOf(it.queryId, ByteString.EMPTY), it.ciphertextsList.single())
            .toByteString()
      }
    }
  }

  override fun makeEncryptedEventData(
    plaintexts: List<DecryptedEventData>,
    joinkeys: List<Pair<Int, String>>
  ): List<EncryptedEventData> {
    return plaintexts.zip(joinkeys).map { (plaintext, joinkeyList) ->
      encryptedEventData {
        queryId = plaintext.queryId
        shardId = plaintext.shardId
        ciphertexts +=
          symmetricCryptor.encrypt(joinkeyList.second.toByteString(), plaintext.plaintext)
      }
    }
  }

  override fun decodeEncryptedQuery(
    data: PCollection<EncryptQueriesResponse>
  ): PCollection<KV<QueryId, ShardedQuery>> {
    return data.parDo("Map to ShardedQuery") {
      yieldAll(
        it
          .ciphertextsList
          .asSequence()
          .map { QueryBundle.parseFrom(it) }
          .flatMap { decodeQueryBundle(it) }
          .map { kvOf(it.queryId, ShardedQuery(it.shardId.id, it.queryId.id, it.bucketId.id)) }
      )
    }
  }
}
