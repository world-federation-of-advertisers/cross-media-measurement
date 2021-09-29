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
import org.wfanet.panelmatch.client.privatemembership.DecryptQueryResultsRequest
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
    request: DecryptQueryResultsRequest
  ): DecryptQueryResultsResponse {
    require(request.serializedParameters != ByteString.EMPTY) {
      "Must set serializedParameters: ${request.serializedParameters}"
    }
    require(request.serializedPublicKey != ByteString.EMPTY) {
      "Must set serializedPublicKey: ${request.serializedPublicKey}"
    }
    require(request.serializedPrivateKey != ByteString.EMPTY) {
      "Must set serializedPrivateKey: ${request.serializedPrivateKey}"
    }
    val decryptedQueryResults =
      request.encryptedQueryResultsList.map {
        privateMembershipCryptorHelper.decodeEncryptedQueryResult(it)
      }
    return decryptQueryResultsResponse {
      eventDataSets +=
        decryptedQueryResults.map { decryptedResult ->
          val eventData = EncryptedEventData.parseFrom(decryptedResult.queryResult)
          decryptedEventDataSet {
            queryId = decryptedResult.queryId
            decryptedEventData +=
              eventData.ciphertextsList.map { ciphertext ->
                plaintext {
                  payload = symmetricCryptor.decrypt(request.singleBlindedJoinkey.key, ciphertext)
                }
              }
          }
        }
    }
  }
}
