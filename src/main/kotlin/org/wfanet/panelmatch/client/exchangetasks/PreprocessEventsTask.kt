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
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.authorizedview.AesGcmCryptoHelper
import org.wfanet.panelmatch.client.authorizedview.encryptedMatchedEvent
import org.wfanet.panelmatch.client.eventpreprocessing.UnprocessedEvent
import org.wfanet.panelmatch.common.Fingerprinters.sha256
import org.wfanet.panelmatch.common.crypto.DeterministicCommutativeCipher
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.parseDelimitedMessages
import org.wfanet.panelmatch.common.storage.toByteString
import org.wfanet.panelmatch.common.toDelimitedByteString

/**
 * Preprocesses events for the BigQuery authorized view workflow with streaming output.
 * 1. Reads UnprocessedEvent protos (plaintext key + DataProviderEvent data)
 * 2. Encrypts keys using EDP's commutative deterministic cipher → G(Key)
 * 3. Hashes the encrypted key for use as join key → H(G(Key))
 * 4. Encrypts event data using AES-GCM with G(Key) as the encryption key
 * 5. Outputs a stream of length-delimited EncryptedMatchedEvent protos
 *
 * Input format (from storage):
 * - Length-delimited UnprocessedEvent messages
 * - Each UnprocessedEvent has:
 *     - id: plaintext join key (e.g., email address, CPN, or Yumi)
 *     - data: serialized DataProviderEvent proto
 *
 * Output format:
 * - Flow of length-delimited EncryptedMatchedEvent protos
 * - Each proto contains:
 *     - encrypted_join_key: H(G(Key)) - SHA256 hash of commutative-encrypted key (for joining)
 *     - encrypted_event_data: AES-GCM encrypted using G(Key) as the encryption key
 *
 * The design ensures that only parties with access to G(Key) can decrypt the event data:
 */
class PreprocessEventsTask(private val cipher: DeterministicCommutativeCipher) : ExchangeTask {

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {
    logger.info("Starting PreprocessEventsTask")
    // Get inputs
    val commutativeEncryptionKey = input.getValue(ENCRYPTION_KEY_LABEL).toByteString()
    val rawEventsData =
      input.getValue(RAW_EVENTS_LABEL).toByteString() // Reads all data into memory

    // Parse all events into a List in memory
    val events: List<UnprocessedEvent> =
      rawEventsData.parseDelimitedMessages(UnprocessedEvent.getDefaultInstance()).toList().filter {
        !it.id.isEmpty && !it.data.isEmpty
      } // Skip invalid events

    logger.info("Processing ${events.size} events")

    return mapOf(
      ENCRYPTED_EVENTS_LABEL to
        flow {
          // Process events in batches for cipher efficiency
          for (eventBatch in events.chunked(CIPHER_BATCH_SIZE)) {
            val batchResults = processBatch(eventBatch, commutativeEncryptionKey)
            emitAll(batchResults.asFlow())
          }
        }
    )
  }

  /** Process a batch of events with efficient cipher operations. */
  private suspend fun processBatch(
    batch: List<UnprocessedEvent>,
    commutativeKey: ByteString,
  ): List<ByteString> {
    // Step 1: Collect plaintext join keys from events
    val plaintextKeys = batch.map { it.id }

    // Step 2: Batch encrypt all keys with commutative cipher
    val edpEncryptedKeys = cipher.encrypt(commutativeKey, plaintextKeys)

    // Step 3: Process each event with its EDP encrypted key
    return batch.zip(edpEncryptedKeys).map { (event, edpEncryptedKey) ->
      // Hash the encrypted key for use as join key: H(G(Key))
      val hashedJoinKey = sha256(edpEncryptedKey)

      // CRITICAL SECURITY: Use G(Key) directly as AES key material
      val encryptedEvent = encryptedMatchedEvent {
        encryptedJoinKey = hashedJoinKey
        this.encryptedEventData = AesGcmCryptoHelper.encrypt(event.data, edpEncryptedKey)
      }
      encryptedEvent.toDelimitedByteString()
    }
  }

  companion object {
    private val logger by loggerFor()
    private const val ENCRYPTION_KEY_LABEL = "encryption-key"
    private const val RAW_EVENTS_LABEL = "raw-events"
    private const val ENCRYPTED_EVENTS_LABEL = "encrypted-events"

    // Batch processing parameters
    private const val CIPHER_BATCH_SIZE = 5000 // Process 5000 keys per JNI call
  }
}
