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
import org.wfanet.panelmatch.client.privatemembership.joinKeyOf
import org.wfanet.panelmatch.client.privatemembership.plaintextOf
import org.wfanet.panelmatch.client.privatemembership.symmetricDecryptQueriesRequest
import org.wfanet.panelmatch.common.toByteString

private val PLAINTEXTS =
  listOf(
    plaintextOf("<some long data a>", 1, 6),
    plaintextOf("<some long data b>", 2, 7),
    plaintextOf("<some long data c>", 3, 8),
    plaintextOf("<some long data d>", 4, 9),
    plaintextOf("<some long data e>", 5, 10)
  )
private val JOINKEYS =
  listOf(
    Pair(1, "some joinkey 1"),
    Pair(2, "some joinkey 1"),
    Pair(3, "some joinkey 1"),
    Pair(4, "some joinkey 1"),
    Pair(5, "some joinkey 1")
  )
private val HKDF_PEPPER = "some-pepper".toByteString()
private val PUBLIC_KEY = "some public key".toByteString()
private val PRIVATE_KEY = "some public key".toByteString()

@RunWith(JUnit4::class)
class PlaintextSymmetricPrivateMembershipCryptorTest {
  val symmetricPrivateMembershipCryptor = PlaintextSymmetricPrivateMembershipCryptor()
  val privateMembershipCryptorHelper = PlaintextPrivateMembershipCryptorHelper

  @Test
  fun `decryptQueries`() {
    val encryptedEventData: List<EncryptedEventData> =
      privateMembershipCryptorHelper.makeEncryptedEventData(PLAINTEXTS, JOINKEYS)
    val encryptedQueryResults =
      privateMembershipCryptorHelper.makeEncryptedQueryResults(encryptedEventData)

    val decryptedQueries =
      encryptedQueryResults.zip(JOINKEYS).map { (encryptedQueryResult, joinkeyList) ->
        val request = symmetricDecryptQueriesRequest {
          singleBlindedJoinkey = joinKeyOf(joinkeyList.second.toByteString())
          this.encryptedQueryResults += encryptedQueryResult
          publicKey = PUBLIC_KEY
          privateKey = PRIVATE_KEY
          hkdfPepper = HKDF_PEPPER
        }
        symmetricPrivateMembershipCryptor
          .decryptQueryResults(request)
          .decryptedEventDataList
          .single()
      }
    assertThat(decryptedQueries).containsExactlyElementsIn(PLAINTEXTS)
  }
}
