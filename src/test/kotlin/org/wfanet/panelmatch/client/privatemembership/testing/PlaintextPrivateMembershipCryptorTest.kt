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

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.privatemembership.QueryBundle
import org.wfanet.panelmatch.client.privatemembership.bucketIdOf
import org.wfanet.panelmatch.client.privatemembership.decryptQueriesRequest
import org.wfanet.panelmatch.client.privatemembership.decryptedQueryOf
import org.wfanet.panelmatch.client.privatemembership.encryptQueriesRequest
import org.wfanet.panelmatch.client.privatemembership.encryptedEventDataOf
import org.wfanet.panelmatch.client.privatemembership.queryBundleOf
import org.wfanet.panelmatch.client.privatemembership.queryIdOf
import org.wfanet.panelmatch.client.privatemembership.shardIdOf
import org.wfanet.panelmatch.client.privatemembership.unencryptedQueryOf
import org.wfanet.panelmatch.common.toByteString

@RunWith(JUnit4::class)
class PlaintextPrivateMembershipCryptorTest {
  val privateMembershipCryptor = PlaintextPrivateMembershipCryptor
  val privateMembershipCryptorHelper = PlaintextPrivateMembershipCryptorHelper

  @Test
  fun `encryptQueries with multiple shards`() {
    val encryptQueriesRequest = encryptQueriesRequest {
      unencryptedQueries +=
        listOf(
          unencryptedQueryOf(100, 1, 1),
          unencryptedQueryOf(100, 2, 2),
          unencryptedQueryOf(101, 3, 1),
          unencryptedQueryOf(101, 4, 5)
        )
    }
    val encryptedQueries = privateMembershipCryptor.encryptQueries(encryptQueriesRequest)
    assertThat(encryptedQueries.ciphertextsList.map { it -> QueryBundle.parseFrom(it) })
      .containsExactly(
        queryBundleOf(shard = 100, listOf(1 to 1, 2 to 2)),
        queryBundleOf(shard = 101, listOf(3 to 1, 4 to 5))
      )
  }

  @Test
  fun `decryptQueries`() {
    val encryptedEventData =
      listOf(
        encryptedEventDataOf("<some encrypted data a>".toByteString(), 1, 6),
        encryptedEventDataOf("<some encrypted data b>".toByteString(), 2, 7),
        encryptedEventDataOf("<some encrypted data c>".toByteString(), 3, 8),
        encryptedEventDataOf("<some encrypted data d>".toByteString(), 4, 9),
        encryptedEventDataOf("<some encrypted data e>".toByteString(), 5, 10)
      )
    val queriedEncryptedResults =
      privateMembershipCryptorHelper.makeEncryptedQueryResults(encryptedEventData)
    val decryptQueriesRequest = decryptQueriesRequest {
      encryptedQueryResults += queriedEncryptedResults
    }

    val decryptedQueries = privateMembershipCryptor.decryptQueryResults(decryptQueriesRequest)
    assertThat(decryptedQueries.decryptedQueryResultsList)
      .containsExactly(
        decryptedQueryOf("<some encrypted data a>".toByteString(), 1, 6),
        decryptedQueryOf("<some encrypted data b>".toByteString(), 2, 7),
        decryptedQueryOf("<some encrypted data c>".toByteString(), 3, 8),
        decryptedQueryOf("<some encrypted data d>".toByteString(), 4, 9),
        decryptedQueryOf("<some encrypted data e>".toByteString(), 5, 10)
      )
  }
}

private fun queryBundleOf(shard: Int, queries: List<Pair<Int, Int>>): QueryBundle {
  return PlaintextQueryEvaluatorTestHelper.makeQueryBundle(
    shardIdOf(shard),
    queries.map { queryIdOf(it.first) to bucketIdOf(it.second) }
  )
}
