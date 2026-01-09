// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.exchangetasks

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.authorizedview.TinkAeadHelper
import org.wfanet.panelmatch.client.authorizedview.DecryptedEventData
import org.wfanet.panelmatch.client.authorizedview.encryptedMatchedEvent
import org.wfanet.panelmatch.client.privatemembership.keyedDecryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.plaintext
import org.wfanet.panelmatch.common.Fingerprinters.sha256
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.parseDelimitedMessages
import org.wfanet.panelmatch.common.storage.toByteString
import org.wfanet.panelmatch.common.toBase64
import org.wfanet.panelmatch.common.toDelimitedByteString

/**
 * Decrypts and matches encrypted events from BigQuery authorized view workflow.
 *
 * Reads EncryptedMatchedEvents containing H(G(Key)), looks up G(Key) and plaintext keys from
 * JoinKeyAndIdCollections, decrypts the event data using AES-GCM with G(Key), and outputs
 * KeyedDecryptedEventDataSet with all extracted event payloads.
 */
class DecryptAndMatchEventsTask : ExchangeTask {

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {
    logger.info("Starting DecryptAndMatchEventsTask")

    val encryptedEventsBlob =
      requireNotNull(input[ENCRYPTED_EVENTS_LABEL]) {
        "Missing required input: $ENCRYPTED_EVENTS_LABEL"
      }

    val plaintextJoinKeysBlob =
      requireNotNull(input[PLAINTEXT_JOIN_KEYS_LABEL]) {
        "Missing required input: $PLAINTEXT_JOIN_KEYS_LABEL"
      }

    val edpEncryptedKeysBlob =
      requireNotNull(input[EDP_ENCRYPTED_KEYS_LABEL]) {
        "Missing required input: $EDP_ENCRYPTED_KEYS_LABEL"
      }

    val plaintextJoinKeyMap = parseJoinKeyMapping(plaintextJoinKeysBlob)
    val edpEncryptedKeyMap = parseJoinKeyMapping(edpEncryptedKeysBlob)
    val keyLookupMap = createKeyLookupMap(plaintextJoinKeyMap, edpEncryptedKeyMap)

    return mapOf(OUTPUT_LABEL to processEncryptedEvents(encryptedEventsBlob, keyLookupMap))
  }

  private suspend fun parseJoinKeyMapping(blob: StorageClient.Blob): Map<ByteString, JoinKeyAndId> =
    JoinKeyAndIdCollection.parseFrom(blob.toByteString()).let { collection ->
      collection.joinKeyAndIdsList.associateBy { it.joinKeyIdentifier.id }
    }

  private fun createKeyLookupMap(
    plaintextMap: Map<ByteString, JoinKeyAndId>,
    edpEncryptedMap: Map<ByteString, JoinKeyAndId>,
  ): Map<ByteString, KeyLookupEntry> =
    edpEncryptedMap.entries.associate { (identifier, edpEncryptedJoinKeyAndId) ->
      val gKey = edpEncryptedJoinKeyAndId.joinKey.key
      val plaintextJoinKeyAndId =
        plaintextMap[identifier]
          ?: throw IllegalStateException(
            "No plaintext mapping found for identifier: '${identifier.toStringUtf8()}'"
          )
      sha256(gKey) to KeyLookupEntry(gKey, plaintextJoinKeyAndId)
    }

  private suspend fun processEncryptedEvents(
    encryptedEventsBlob: StorageClient.Blob,
    keyLookupMap: Map<ByteString, KeyLookupEntry>,
  ): Flow<ByteString> = flow {
    val encryptedEvents =
      encryptedEventsBlob.toByteString().parseDelimitedMessages(encryptedMatchedEvent {})

    var processedCount = 0

    for (encryptedEvent in encryptedEvents) {
      val hGKey = encryptedEvent.encryptedJoinKey

      val keyEntry =
        requireNotNull(keyLookupMap[hGKey]) {
          "No key mapping found for H(G(Key)): ${hGKey.toBase64()}"
        }

      val decryptedBytes =
        TinkAeadHelper.decrypt(encryptedEvent.encryptedEventData, keyEntry.gKey)
      val decryptedEventDataProto = DecryptedEventData.parseFrom(decryptedBytes)

      val output = keyedDecryptedEventDataSet {
        plaintextJoinKeyAndId = keyEntry.plaintextJoinKeyAndId
        decryptedEventDataProto.eventDataList.forEach { eventBytes ->
          decryptedEventData += plaintext { payload = eventBytes }
        }
      }
      emit(output.toDelimitedByteString())
      processedCount++
    }

    logger.info("Completed DecryptAndMatchEventsTask - processed: $processedCount")
  }

  companion object {
    private val logger by loggerFor()
    private const val ENCRYPTED_EVENTS_LABEL = "encrypted-events"
    private const val PLAINTEXT_JOIN_KEYS_LABEL = "plaintext-join-keys"
    private const val EDP_ENCRYPTED_KEYS_LABEL = "edp-encrypted-keys"
    private const val OUTPUT_LABEL = "decrypted-event-data"
  }

  private data class KeyLookupEntry(val gKey: ByteString, val plaintextJoinKeyAndId: JoinKeyAndId)
}
