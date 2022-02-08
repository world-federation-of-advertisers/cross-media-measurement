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
import com.google.protobuf.StringValue
import org.wfanet.panelmatch.client.privatemembership.DecryptQueryResultsParameters
import org.wfanet.panelmatch.client.privatemembership.DecryptQueryResultsResponse
import org.wfanet.panelmatch.client.privatemembership.EncryptedEventData
import org.wfanet.panelmatch.client.privatemembership.QueryResultsDecryptor
import org.wfanet.panelmatch.client.privatemembership.decryptQueryResultsResponse
import org.wfanet.panelmatch.client.privatemembership.decryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.plaintext
import org.wfanet.panelmatch.common.crypto.SymmetricCryptor
import org.wfanet.panelmatch.common.crypto.testing.FakeSymmetricCryptor

class PlaintextQueryResultsDecryptor(
  private val privateMembershipCryptorHelper: PrivateMembershipCryptorHelper =
    PlaintextPrivateMembershipCryptorHelper(),
  private val symmetricCryptor: SymmetricCryptor = FakeSymmetricCryptor(),
) : QueryResultsDecryptor {

  override fun decryptQueryResults(
    parameters: DecryptQueryResultsParameters
  ): DecryptQueryResultsResponse {
    require(parameters.parameters.unpack(StringValue::class.java).value != "") {
      "Must set serializedParameters: ${parameters.parameters}"
    }
    require(parameters.serializedPublicKey != ByteString.EMPTY) {
      "Must set serializedPublicKey: ${parameters.serializedPublicKey}"
    }
    require(parameters.serializedPrivateKey != ByteString.EMPTY) {
      "Must set serializedPrivateKey: ${parameters.serializedPrivateKey}"
    }
    val decryptedQueryResults =
      parameters.encryptedQueryResults.map {
        privateMembershipCryptorHelper.decodeEncryptedQueryResult(it)
      }
    return decryptQueryResultsResponse {
      eventDataSets +=
        decryptedQueryResults.map { decryptedResult ->
          val eventData =
            EncryptedEventData.parseFrom(decryptedResult.queryResult.itemsList.single())
          decryptedEventDataSet {
            queryId = decryptedResult.queryId
            decryptedEventData +=
              eventData.ciphertextsList.map { ciphertext ->
                plaintext {
                  payload =
                    symmetricCryptor
                      .decrypt(parameters.decryptedJoinKey.key, listOf(ciphertext))
                      .single()
                }
              }
          }
        }
    }
  }
}
