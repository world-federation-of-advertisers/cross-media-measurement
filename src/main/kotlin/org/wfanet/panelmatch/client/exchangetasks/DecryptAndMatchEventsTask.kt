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
import java.security.MessageDigest
import java.util.Base64
import java.util.logging.Logger
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.authorizedview.encryptedMatchedEvent
import org.wfanet.panelmatch.client.privatemembership.keyedDecryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.plaintext
import org.wfanet.panelmatch.common.parseDelimitedMessages
import org.wfanet.panelmatch.common.storage.newInputStream
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

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val ENCRYPTED_EVENTS_LABEL = "encrypted-events"
    private const val PLAINTEXT_JOIN_KEYS_LABEL = "plaintext-join-keys"
    private const val EDP_ENCRYPTED_KEYS_LABEL = "edp-encrypted-keys"
    private const val OUTPUT_LABEL = "decrypted-event-data"

    // AES-GCM parameters
    private const val AES_KEY_SIZE = 256 / 8 // 32 bytes
    private const val GCM_IV_SIZE = 12 // 96 bits
    private const val GCM_TAG_SIZE = 128 // 128 bits
  }

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
    withContext(Dispatchers.IO) {
      blob
        .newInputStream(this@withContext)
        .use { stream -> JoinKeyAndIdCollection.parseFrom(stream.readBytes()) }
        .let { collection ->
          collection.joinKeyAndIdsList.associateBy { joinKeyAndId ->
            joinKeyAndId.joinKeyIdentifier.id
          }
        }
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
    withContext(Dispatchers.IO) {
      encryptedEventsBlob.newInputStream(this).use { inputStream ->
        // Parse the stream of length-delimited EncryptedMatchedEvent messages
        val eventStream = inputStream.parseDelimitedMessages(encryptedMatchedEvent {})

        // Process each event as it's parsed from the stream
        for (encryptedMatchedEvent in eventStream) {
          // Look up the plaintext JoinKeyAndId using raw bytes
          val encryptedKey = encryptedMatchedEvent.encryptedJoinKey
          val plaintextJoinKeyAndId = keyLookupMap[encryptedKey]
          if (plaintextJoinKeyAndId == null) {
            val keyBase64 = Base64.getEncoder().encodeToString(encryptedKey.toByteArray())
            logger.warning("No mapping found for encrypted key: $keyBase64")
            continue
          }

          // Decrypt the event data using raw bytes
          val decryptedDataResult = runCatching {
            decryptEventData(encryptedKey.toByteArray(), encryptedMatchedEvent.encryptedEventData)
          }

          val decryptedData = decryptedDataResult.getOrNull()
          if (decryptedData == null) {
            logger.warning(
              "Failed to decrypt event data for key: ${Base64.getEncoder().encodeToString(encryptedKey.toByteArray())} - ${decryptedDataResult.exceptionOrNull()?.message}"
            )
            continue
          }

          // Create KeyedDecryptedEventDataSet (matching private membership output format)
          val output = keyedDecryptedEventDataSet {
            this.plaintextJoinKeyAndId = plaintextJoinKeyAndId
            this.decryptedEventData += plaintext { payload = decryptedData }
          }
          this@flow.emit(output.toDelimitedByteString())
        }
      }
    }
    logger.info("Completed DecryptAndMatchEventsTask")
  }

  /**
   * Decrypts event data using AES-GCM with raw bytes. The AES key is derived from SHA-256 of the
   * raw encrypted join key bytes. The encrypted data format is: IV (12 bytes) + ciphertext + auth
   * tag (16 bytes)
   */
  private fun decryptEventData(encryptedJoinKey: ByteArray, encryptedData: ByteString): ByteString {
    // Derive AES key from raw encrypted join key bytes
    val aesKey = deriveAesKey(encryptedJoinKey)

    // Extract IV, ciphertext, and auth tag
    val encryptedBytes = encryptedData.toByteArray()

    if (encryptedBytes.size < GCM_IV_SIZE + GCM_TAG_SIZE / 8) {
      throw IllegalArgumentException("Encrypted data too short: ${encryptedBytes.size} bytes")
    }

    val iv = encryptedBytes.sliceArray(0 until GCM_IV_SIZE)
    val ciphertextWithTag = encryptedBytes.sliceArray(GCM_IV_SIZE until encryptedBytes.size)

    // Decrypt using AES-GCM
    val cipher = Cipher.getInstance("AES/GCM/NoPadding")
    val keySpec = SecretKeySpec(aesKey, "AES")
    val gcmSpec = GCMParameterSpec(GCM_TAG_SIZE, iv)

    cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec)
    val plaintext = cipher.doFinal(ciphertextWithTag)

    return ByteString.copyFrom(plaintext)
  }

  /**
   * Derives AES key from raw encrypted join key bytes using SHA-256. Uses raw bytes directly, not
   * base64 string representation.
   */
  private fun deriveAesKey(encryptedJoinKey: ByteArray): ByteArray {
    val digest = MessageDigest.getInstance("SHA-256")
    val hash = digest.digest(encryptedJoinKey)

    // Use first 32 bytes of hash as AES-256 key
    return hash.sliceArray(0 until AES_KEY_SIZE)
  }
}
