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

package org.wfanet.panelmatch.integration.testing

import org.wfanet.panelmatch.client.eventpreprocessing.CombinedEvents
import org.wfanet.panelmatch.client.privatemembership.KeyedDecryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.isPaddingQuery

const val TEST_PADDING_NONCE_PREFIX: String = "[Padding Nonce]"

data class ParsedPlaintextResults(
  val joinKey: String,
  val isPaddingQuery: Boolean,
  val plaintexts: List<String>,
)

/**
 * Parses plaintext results from a [KeyedDecryptedEventDataSet] containing serialized
 * [CombinedEvents] as its data.
 */
fun parsePlaintextResults(
  combinedTexts: Iterable<KeyedDecryptedEventDataSet>
): List<ParsedPlaintextResults> {
  return combinedTexts.map { keyedDecryptedEventDataSet ->
    val keyAndId = keyedDecryptedEventDataSet.plaintextJoinKeyAndId
    val joinKey = requireNotNull(keyAndId.joinKey).key.toStringUtf8()
    val isPaddingQuery = keyAndId.joinKeyIdentifier.isPaddingQuery
    val payload =
      if (isPaddingQuery) {
        val element = keyedDecryptedEventDataSet.decryptedEventDataList.single().payload
        listOf("$TEST_PADDING_NONCE_PREFIX ${element.toStringUtf8()}")
      } else {
        keyedDecryptedEventDataSet.decryptedEventDataList.flatMap { plaintext ->
          CombinedEvents.parseFrom(plaintext.payload).serializedEventsList.map { serializedEvent ->
            serializedEvent.toStringUtf8()
          }
        }
      }
    ParsedPlaintextResults(joinKey = joinKey, isPaddingQuery = isPaddingQuery, plaintexts = payload)
  }
}
