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
import java.util.Base64
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.authorizedview.AesGcmCryptoHelper
import org.wfanet.panelmatch.client.authorizedview.encryptedMatchedEvent
import org.wfanet.panelmatch.client.privatemembership.keyedDecryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.plaintext
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.parseDelimitedMessages
import org.wfanet.panelmatch.common.storage.toByteString
import org.wfanet.panelmatch.common.toDelimitedByteString

/**
 * Decrypts and matches encrypted events from BigQuery authorized view workflow.
 *
 * This task:
 * 1. Reads encrypted events from BigQuery (via ReadEncryptedEventsFromBigQueryTask)
 * 2. Maps encrypted keys to plaintext keys using JoinKeyAndIdCollection
 * 3. Decrypts event data using AES-GCM with raw bytes
 * 4. Outputs KeyedDecryptedEventDataSet matching private membership format
 *
 * The design uses raw bytes internally and only converts at boundaries:
 * - Input: ByteString (raw bytes) from proto
 * - Processing: Raw bytes for AES key derivation
 * - Map keys: ByteString for consistency
 */
class DecryptAndMatchEventsTask : ExchangeTask {

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> =
    withContext(Dispatchers.IO) {
      logger.info("Starting DecryptAndMatchEventsTask")

      // Read encrypted events
      val encryptedEventsBlob =
        requireNotNull(input[ENCRYPTED_EVENTS_LABEL]) {
          "Missing required input: $ENCRYPTED_EVENTS_LABEL"
        }

      // Read plaintext join key mappings
      val plaintextJoinKeysBlob =
        requireNotNull(input[PLAINTEXT_JOIN_KEYS_LABEL]) {
          "Missing required input: $PLAINTEXT_JOIN_KEYS_LABEL"
        }

      // Read EDP-encrypted join key mappings
      val edpEncryptedKeysBlob =
        requireNotNull(input[EDP_ENCRYPTED_KEYS_LABEL]) {
          "Missing required input: $EDP_ENCRYPTED_KEYS_LABEL"
        }

      // Parse join key mappings
      val plaintextJoinKeyMap = parseJoinKeyMapping(plaintextJoinKeysBlob)
      val edpEncryptedKeyMap = parseJoinKeyMapping(edpEncryptedKeysBlob)

      // Create key lookup map: encrypted_key -> (plaintext_key, identifier)
      val keyLookupMap = createKeyLookupMap(plaintextJoinKeyMap, edpEncryptedKeyMap)

      // Process encrypted events
      val outputFlow = processEncryptedEvents(encryptedEventsBlob, keyLookupMap)

      mapOf(OUTPUT_LABEL to outputFlow)
    }

  /**
   * Parses JoinKeyAndIdCollection from blob and maps by join key. Uses ByteString as map key to
   * work with raw bytes throughout.
   */
  private suspend fun parseJoinKeyMapping(blob: StorageClient.Blob): Map<ByteString, JoinKeyAndId> =
    JoinKeyAndIdCollection.parseFrom(blob.toByteString()).let { collection ->
      collection.joinKeyAndIdsList.associateBy { joinKeyAndId -> joinKeyAndId.joinKeyIdentifier.id }
    }

  /**
   * Creates a lookup map from encrypted keys to plaintext JoinKeyAndId. This enables mapping from
   * the encrypted keys in BigQuery to the original plaintext keys.
   */
  private fun createKeyLookupMap(
    plaintextMap: Map<ByteString, JoinKeyAndId>,
    edpEncryptedMap: Map<ByteString, JoinKeyAndId>,
  ): Map<ByteString, JoinKeyAndId> {
    // Create a map from EDP-encrypted key to the original plaintext JoinKeyAndId.
    // This is done by joining edpEncryptedMap and plaintextMap on the common identifier.
    return edpEncryptedMap
      .map { (identifier, encryptedJoinKeyAndId) ->
        val encryptedKey = encryptedJoinKeyAndId.joinKey.key
        val plaintextJoinKeyAndId =
          requireNotNull(plaintextMap[identifier]) {
            "No plaintext mapping found for identifier: '${identifier.toStringUtf8()}'"
          }
        encryptedKey to plaintextJoinKeyAndId
      }
      .toMap()
  }

  /**
   * Processes a stream of length-delimited EncryptedMatchedEvent protos and outputs
   * KeyedDecryptedEventDataSet protos.
   */
  private suspend fun processEncryptedEvents(
    encryptedEventsBlob: StorageClient.Blob,
    keyLookupMap: Map<ByteString, JoinKeyAndId>,
  ): Flow<ByteString> = flow {
    val encryptedEvents =
      encryptedEventsBlob.toByteString().parseDelimitedMessages(encryptedMatchedEvent {})
    for (encryptedEvent in encryptedEvents) {
      val encryptedKey = encryptedEvent.encryptedJoinKey
      val plaintextJoinKeyAndId = keyLookupMap[encryptedKey]
      if (plaintextJoinKeyAndId == null) {
        val keyBase64 = Base64.getEncoder().encodeToString(encryptedKey.toByteArray())
        logger.warning("No mapping found for encrypted key: $keyBase64")
        continue
      }
      // Decrypt the event data using the centralized crypto helper
      val decryptedData =
        try {
          AesGcmCryptoHelper.decryptWithDerivedKey(
            encryptedEvent.encryptedEventData,
            encryptedKey.toByteArray(),
          )
        } catch (e: Exception) {
          logger.warning(
            "Failed to decrypt event data for key: ${Base64.getEncoder().encodeToString(encryptedKey.toByteArray())} - ${e.message}"
          )
          continue
        }

      // Create KeyedDecryptedEventDataSet (matching private membership output format)
      val output = keyedDecryptedEventDataSet {
        this.plaintextJoinKeyAndId = plaintextJoinKeyAndId
        this.decryptedEventData += plaintext { payload = decryptedData }
      }
      emit(output.toDelimitedByteString())
    }
    logger.info("Completed DecryptAndMatchEventsTask")
  }

  companion object {
    private val logger by loggerFor()
    private const val ENCRYPTED_EVENTS_LABEL = "encrypted-events"
    private const val PLAINTEXT_JOIN_KEYS_LABEL = "plaintext-join-keys"
    private const val EDP_ENCRYPTED_KEYS_LABEL = "edp-encrypted-keys"
    private const val OUTPUT_LABEL = "decrypted-event-data"
  }
}
