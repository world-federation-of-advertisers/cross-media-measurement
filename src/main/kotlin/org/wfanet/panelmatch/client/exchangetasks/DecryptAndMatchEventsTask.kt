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
import org.wfanet.panelmatch.common.Fingerprinters.sha256

/**
 * Decrypts and matches encrypted events from BigQuery authorized view workflow.
 *
 * This task:
 * 1. Reads encrypted events from input Blob
 * 2. Maps H(G(Key)) from events to G(Key) and plaintext keys using JoinKeyAndIdCollections
 * 3. Decrypts event data using AES-GCM with G(Key)
 * 4. Outputs KeyedDecryptedEventDataSet
 *
 * Security Note: Events contain H(G(Key)) for joining, but decryption requires G(Key). This ensures
 * only parties with G(Key) can decrypt, not those with just H(G(Key)).
 *
 * The design uses raw bytes internally and only converts at boundaries:
 * - Input: ByteString (raw bytes) from proto
 * - Processing: Raw bytes for AES encryption
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

      // Create key lookup maps: H(G(Key)) -> G(Key) and H(G(Key)) -> plaintext
      val (hashToGKeyMap, hashToPlaintextMap) =
        createKeyLookupMaps(plaintextJoinKeyMap, edpEncryptedKeyMap)

      // Process encrypted events
      val outputFlow =
        processEncryptedEvents(encryptedEventsBlob, hashToGKeyMap, hashToPlaintextMap)

      mapOf(OUTPUT_LABEL to outputFlow)
    }

  /** Parses JoinKeyAndIdCollection from blob and maps by join key. */
  private suspend fun parseJoinKeyMapping(blob: StorageClient.Blob): Map<ByteString, JoinKeyAndId> =
    JoinKeyAndIdCollection.parseFrom(blob.toByteString()).let { collection ->
      collection.joinKeyAndIdsList.associateBy { joinKeyAndId -> joinKeyAndId.joinKeyIdentifier.id }
    }

  /**
   * Creates lookup maps for decryption and matching. Returns a pair of:
   * 1. Map from H(G(Key)) to G(Key) for decryption
   * 2. Map from H(G(Key)) to plaintext JoinKeyAndId for output
   */
  private fun createKeyLookupMaps(
    plaintextMap: Map<ByteString, JoinKeyAndId>,
    edpEncryptedMap: Map<ByteString, JoinKeyAndId>,
  ): Pair<Map<ByteString, ByteString>, Map<ByteString, JoinKeyAndId>> {
    val hashToGKeyMap = mutableMapOf<ByteString, ByteString>()
    val hashToPlaintextMap = mutableMapOf<ByteString, JoinKeyAndId>()

    // Join the two maps on common identifier
    for ((identifier, edpEncryptedJoinKeyAndId) in edpEncryptedMap) {
      val gKey = edpEncryptedJoinKeyAndId.joinKey.key
      val hGKey = sha256(gKey)

      val plaintextJoinKeyAndId =
        plaintextMap[identifier]
          ?: throw IllegalStateException(
            "No plaintext mapping found for identifier: '${identifier.toStringUtf8()}'"
          )

      // Map H(G(Key)) to G(Key) for decryption
      hashToGKeyMap[hGKey] = gKey
      // Map H(G(Key)) to plaintext for output
      hashToPlaintextMap[hGKey] = plaintextJoinKeyAndId
    }

    return hashToGKeyMap to hashToPlaintextMap
  }

  /**
   * Processes a blob of length-delimited EncryptedMatchedEvent protos and outputs
   * KeyedDecryptedEventDataSet protos.
   *
   * @param encryptedEventsBlob Blob containing encrypted events with H(G(Key)) as join keys
   * @param hashToGKeyMap Map from H(G(Key)) to G(Key) for decryption
   * @param hashToPlaintextMap Map from H(G(Key)) to plaintext JoinKeyAndId for output
   */
  private suspend fun processEncryptedEvents(
    encryptedEventsBlob: StorageClient.Blob,
    hashToGKeyMap: Map<ByteString, ByteString>,
    hashToPlaintextMap: Map<ByteString, JoinKeyAndId>,
  ): Flow<ByteString> = flow {
    val encryptedEvents =
      encryptedEventsBlob.toByteString().parseDelimitedMessages(encryptedMatchedEvent {})

    var processedCount = 0
    var skippedCount = 0

    for (encryptedEvent in encryptedEvents) {
      // Event contains H(G(Key)) as the join key
      val hGKey = encryptedEvent.encryptedJoinKey

      // Look up G(Key) needed for decryption
      val gKey = hashToGKeyMap[hGKey]
      if (gKey == null) {
        logger.warning("No G(Key) mapping found for H(G(Key)): $hGKey.toBase64()")
        skippedCount++
        continue
      }

      // Look up plaintext JoinKeyAndId for output
      val plaintextJoinKeyAndId = hashToPlaintextMap[hGKey]
      if (plaintextJoinKeyAndId == null) {
        logger.warning("No plaintext mapping found for H(G(Key)): $hGKey.toBase64()")
        skippedCount++
        continue
      }

      // Decrypt the event data using raw G(Key) (NOT H(G(Key)))
      // This is the critical security property - only parties with G(Key) can decrypt
      val decryptedData =
        try {
          AesGcmCryptoHelper.decrypt(encryptedEvent.encryptedEventData, gKey)
        } catch (e: Exception) {
          logger.warning(
            "Failed to decrypt event data for H(G(Key)): ${Base64.getEncoder().encodeToString(hGKey.toByteArray())} - ${e.message}"
          )
          skippedCount++
          continue
        }

      // Create KeyedDecryptedEventDataSet
      val output = keyedDecryptedEventDataSet {
        this.plaintextJoinKeyAndId = plaintextJoinKeyAndId
        this.decryptedEventData += plaintext { payload = decryptedData }
      }
      emit(output.toDelimitedByteString())
      processedCount++
    }

    logger.info(
      "Completed DecryptAndMatchEventsTask - processed: $processedCount, skipped: $skippedCount"
    )
  }

  companion object {
    private val logger by loggerFor()
    private const val ENCRYPTED_EVENTS_LABEL = "encrypted-events"
    private const val PLAINTEXT_JOIN_KEYS_LABEL = "plaintext-join-keys"
    private const val EDP_ENCRYPTED_KEYS_LABEL = "edp-encrypted-keys"
    private const val OUTPUT_LABEL = "decrypted-event-data"

    private val BASE64_ENCODER: Base64.Encoder = Base64.getEncoder()

    private fun ByteString.toBase64(): String {
      if (this.isEmpty) return ""
      val byteBuffer = this.asReadOnlyByteBuffer()
      return BASE64_ENCODER.encodeToString(this.toByteArray())
    }
  }
}
