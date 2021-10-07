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
import org.wfanet.panelmatch.client.privatemembership.EncryptedEventData
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.bucketIdOf
import org.wfanet.panelmatch.client.privatemembership.decryptedQueryOf
import org.wfanet.panelmatch.client.privatemembership.queryIdOf
import org.wfanet.panelmatch.client.privatemembership.shardIdOf
import org.wfanet.panelmatch.common.toByteString

private val SERIALIZED_PARAMETERS = "some-serialized-parameters".toByteString()

@RunWith(JUnit4::class)
class PlaintextPrivateMembershipCryptorTest {
  val privateMembershipCryptor = PlaintextPrivateMembershipCryptor(SERIALIZED_PARAMETERS)
  val privateMembershipCryptorHelper = PlaintextPrivateMembershipCryptorHelper()

  @Test
  fun `encryptQueries with multiple shards`() {
    val keys = privateMembershipCryptor.generateKeys()
    val unencryptedQueriesList =
      listOf(
        listOf(
          unencryptedQueryOf(100, 1, 1),
          unencryptedQueryOf(100, 2, 2),
        ),
        listOf(
          unencryptedQueryOf(101, 3, 1),
          unencryptedQueryOf(101, 4, 5),
        )
      )
    val encryptedQueriesList =
      unencryptedQueriesList.map { privateMembershipCryptor.encryptQueries(it, keys) }
    assertThat(encryptedQueriesList.map { EncryptedQueryBundle.parseFrom(it) })
      .containsExactly(
        encryptedQueryBundleOf(shard = 100, listOf(1 to 1, 2 to 2)),
        encryptedQueryBundleOf(shard = 101, listOf(3 to 1, 4 to 5))
      )
  }

  @Test
  fun decryptQueries() {
    val keys = privateMembershipCryptor.generateKeys()
    val encryptedEventData =
      listOf(
        encryptedEventDataSetOf(
          listOf(
            "<some encrypted data a>",
            "<some encrypted data b>",
          ),
          1
        ),
        encryptedEventDataSetOf(
          listOf(
            "<some encrypted data c>",
            "<some encrypted data d>",
          ),
          2
        ),
        encryptedEventDataSetOf(
          listOf(
            "<some encrypted data e>",
          ),
          3
        ),
      )
    val encryptedQueryResults =
      encryptedEventData.map { privateMembershipCryptorHelper.makeEncryptedQueryResult(keys, it) }

    val decryptedQueries =
      encryptedQueryResults.map { privateMembershipCryptorHelper.decodeEncryptedQueryResult(it) }
    assertThat(
        decryptedQueries
          .map { decryptedQueryData ->
            EncryptedEventData.parseFrom(decryptedQueryData.queryResult.itemsList.single())
              .ciphertextsList
              .map { decryptedQueryOf(decryptedQueryData.queryId, listOf(it)) }
          }
          .flatten()
      )
      .containsExactly(
        decryptedQueryOf(1, "<some encrypted data a>".toByteString()),
        decryptedQueryOf(1, "<some encrypted data b>".toByteString()),
        decryptedQueryOf(2, "<some encrypted data c>".toByteString()),
        decryptedQueryOf(2, "<some encrypted data d>".toByteString()),
        decryptedQueryOf(3, "<some encrypted data e>".toByteString())
      )
  }

  private fun encryptedQueryBundleOf(
    shard: Int,
    queries: List<Pair<Int, Int>>
  ): EncryptedQueryBundle {
    return privateMembershipCryptorHelper.makeEncryptedQueryBundle(
      shardIdOf(shard),
      queries.map { queryIdOf(it.first) to bucketIdOf(it.second) }
    )
  }
}
