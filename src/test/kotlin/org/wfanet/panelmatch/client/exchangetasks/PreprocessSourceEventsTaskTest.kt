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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.authorizedview.AesGcmCryptoHelper
import org.wfanet.panelmatch.client.authorizedview.EncryptedMatchedEvent
import org.wfanet.panelmatch.client.authorizedview.encryptedMatchedEvent
import org.wfanet.panelmatch.client.eventpreprocessing.preprocessEventsRequest
import org.wfanet.panelmatch.client.eventpreprocessing.unprocessedEvent
import org.wfanet.panelmatch.client.exchangetasks.testing.executeToByteStrings
import org.wfanet.panelmatch.common.crypto.testing.FakeDeterministicCommutativeCipher
import org.wfanet.panelmatch.common.parseDelimitedMessages

@RunWith(JUnit4::class)
class PreprocessSourceEventsTaskTest {

  private val cipher = FakeDeterministicCommutativeCipher
  private val encryptionKey = cipher.generateKey()

  private fun createTestRequest(vararg events: Pair<String, String>) = preprocessEventsRequest {
    cryptoKey = encryptionKey
    for ((id, data) in events) {
      unprocessedEvents += unprocessedEvent {
        this.id = id.toByteStringUtf8()
        this.data = data.toByteStringUtf8()
      }
    }
  }

  private fun executePreprocess(
    request: ByteString,
    key: ByteString = encryptionKey,
  ): Map<String, ByteString> {
    return PreprocessSourceEventsTask(cipher)
      .executeToByteStrings("encryption-key" to key, "raw-events" to request)
  }

  private fun parseEncryptedEvents(byteString: ByteString): List<EncryptedMatchedEvent> {
    return byteString.parseDelimitedMessages(encryptedMatchedEvent {}).toList()
  }

  @Test
  fun `successfully preprocesses single event`() {
    val request = createTestRequest("user1" to "event-data-1")

    val result = executePreprocess(request.toByteString())

    assertThat(result).containsKey("encrypted-events")
    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    assertThat(encryptedEvents).hasSize(1)

    val event = encryptedEvents[0]
    // Verify join key is encrypted
    val expectedEncryptedKey = cipher.encrypt(encryptionKey, listOf("user1".toByteStringUtf8()))[0]
    assertThat(event.encryptedJoinKey).isEqualTo(expectedEncryptedKey)

    // Verify event data can be decrypted
    val derivedKey = AesGcmCryptoHelper.deriveAesKey(expectedEncryptedKey.toByteArray())
    val decryptedData = AesGcmCryptoHelper.decrypt(event.encryptedEventData, derivedKey)
    assertThat(decryptedData).isEqualTo("event-data-1".toByteStringUtf8())
  }

  @Test
  fun `successfully preprocesses multiple events in batch`() {
    val request =
      createTestRequest(
        "user1" to "event-data-1",
        "user2" to "event-data-2",
        "user3" to "event-data-3",
      )

    val result = executePreprocess(request.toByteString())

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    assertThat(encryptedEvents).hasSize(3)

    // Verify each event
    val expectedIds = listOf("user1", "user2", "user3")
    val expectedData = listOf("event-data-1", "event-data-2", "event-data-3")

    for ((index, event) in encryptedEvents.withIndex()) {
      // Verify join key encryption
      val expectedEncryptedKey =
        cipher.encrypt(encryptionKey, listOf(expectedIds[index].toByteStringUtf8()))[0]
      assertThat(event.encryptedJoinKey).isEqualTo(expectedEncryptedKey)

      // Verify data decryption
      val derivedKey = AesGcmCryptoHelper.deriveAesKey(expectedEncryptedKey.toByteArray())
      val decryptedData = AesGcmCryptoHelper.decrypt(event.encryptedEventData, derivedKey)
      assertThat(decryptedData).isEqualTo(expectedData[index].toByteStringUtf8())
    }
  }

  @Test
  fun `handles empty events list`() {
    val request = preprocessEventsRequest {
      cryptoKey = encryptionKey
      // No events added
    }

    val result = executePreprocess(request.toByteString())

    assertThat(result).containsKey("encrypted-events")
    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    assertThat(encryptedEvents).isEmpty()
  }

  @Test
  fun `fails with missing encryption key`() {
    val request = createTestRequest("user1" to "event-data")

    val exception =
      assertFailsWith<IllegalArgumentException> {
        PreprocessSourceEventsTask(cipher)
          .executeToByteStrings(
            "raw-events" to request.toByteString()
            // Missing "encryption-key"
          )
      }

    assertThat(exception.message).contains("encryption-key")
  }

  @Test
  fun `fails with missing raw events`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        PreprocessSourceEventsTask(cipher)
          .executeToByteStrings(
            "encryption-key" to encryptionKey
            // Missing "raw-events"
          )
      }

    assertThat(exception.message).contains("raw-events")
  }

  @Test
  fun `produces valid EncryptedMatchedEvent protos`() {
    val request = createTestRequest("user1" to "event-data")

    val result = executePreprocess(request.toByteString())

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    val event = encryptedEvents[0]

    // Verify proto structure
    assertThat(event.encryptedJoinKey.isEmpty).isFalse()
    assertThat(event.encryptedEventData.isEmpty).isFalse()

    // Encrypted data should be larger than original due to IV and auth tag
    assertThat(event.encryptedEventData.size()).isGreaterThan("event-data".length)
  }

  @Test
  fun `encrypted join keys are deterministic`() {
    val request =
      createTestRequest(
        "user1" to "event-data-1",
        "user1" to "event-data-2", // Same user ID
      )

    val result = executePreprocess(request.toByteString())

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    assertThat(encryptedEvents).hasSize(2)

    // Same join key should produce same encrypted join key
    assertThat(encryptedEvents[0].encryptedJoinKey).isEqualTo(encryptedEvents[1].encryptedJoinKey)
  }

  @Test
  fun `different join keys produce different encrypted keys`() {
    val request =
      createTestRequest(
        "user1" to "event-data",
        "user2" to "event-data", // Different user ID, same data
      )

    val result = executePreprocess(request.toByteString())

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    assertThat(encryptedEvents).hasSize(2)

    // Different join keys should produce different encrypted keys
    assertThat(encryptedEvents[0].encryptedJoinKey)
      .isNotEqualTo(encryptedEvents[1].encryptedJoinKey)
  }

  @Test
  fun `event data encryption uses correct AES key derivation`() {
    val userId = "test-user"
    val eventData = "sensitive-event-data"
    val request = createTestRequest(userId to eventData)

    val result = executePreprocess(request.toByteString())

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    val event = encryptedEvents[0]

    // Manually verify the encryption flow
    val expectedEncryptedJoinKey =
      cipher.encrypt(encryptionKey, listOf(userId.toByteStringUtf8()))[0]
    assertThat(event.encryptedJoinKey).isEqualTo(expectedEncryptedJoinKey)

    // AES key should be derived from encrypted join key
    val derivedAesKey = AesGcmCryptoHelper.deriveAesKey(expectedEncryptedJoinKey.toByteArray())

    // Decrypting with the derived key should give us the original data
    val decryptedData = AesGcmCryptoHelper.decrypt(event.encryptedEventData, derivedAesKey)
    assertThat(decryptedData).isEqualTo(eventData.toByteStringUtf8())

    // Using a different key should fail
    val wrongKey = AesGcmCryptoHelper.deriveAesKey("wrong-key".toByteArray())
    assertFailsWith<javax.crypto.AEADBadTagException> {
      AesGcmCryptoHelper.decrypt(event.encryptedEventData, wrongKey)
    }
  }

  @Test
  fun `handles large batch processing`() {
    // Create a request with 1000 events
    val eventCount = 1000
    val events = (1..eventCount).map { i -> "user$i" to "event-data-$i" }.toTypedArray()

    val request = createTestRequest(*events)

    val result = executePreprocess(request.toByteString())

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    assertThat(encryptedEvents).hasSize(eventCount)

    // Verify first and last events
    val firstEvent = encryptedEvents.first()
    val lastEvent = encryptedEvents.last()

    // First event
    val firstExpectedKey = cipher.encrypt(encryptionKey, listOf("user1".toByteStringUtf8()))[0]
    assertThat(firstEvent.encryptedJoinKey).isEqualTo(firstExpectedKey)

    // Last event
    val lastExpectedKey =
      cipher.encrypt(encryptionKey, listOf("user$eventCount".toByteStringUtf8()))[0]
    assertThat(lastEvent.encryptedJoinKey).isEqualTo(lastExpectedKey)
  }

  @Test
  fun `skips events with empty data`() {
    val request =
      createTestRequest(
        "user1" to "", // Empty event data - will be skipped
        "user2" to "non-empty-data",
      )

    val result = executePreprocess(request.toByteString())

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    // Only one event should be processed (user2 with non-empty data)
    assertThat(encryptedEvents).hasSize(1)

    // Verify the non-empty data event is processed correctly
    val event = encryptedEvents[0]
    val encryptedKey = cipher.encrypt(encryptionKey, listOf("user2".toByteStringUtf8()))[0]
    val derivedKey = AesGcmCryptoHelper.deriveAesKey(encryptedKey.toByteArray())
    val decryptedData = AesGcmCryptoHelper.decrypt(event.encryptedEventData, derivedKey)
    assertThat(decryptedData).isEqualTo("non-empty-data".toByteStringUtf8())
  }

  @Test
  fun `handles events with binary data`() {
    // Create binary data (not UTF-8 text)
    val binaryData = ByteArray(256) { it.toByte() }
    val request = preprocessEventsRequest {
      cryptoKey = encryptionKey
      unprocessedEvents += unprocessedEvent {
        id = "binary-user".toByteStringUtf8()
        data = binaryData.toByteString()
      }
    }

    val result = executePreprocess(request.toByteString())

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    val event = encryptedEvents[0]

    // Verify binary data is preserved
    val encryptedKey = cipher.encrypt(encryptionKey, listOf("binary-user".toByteStringUtf8()))[0]
    val derivedKey = AesGcmCryptoHelper.deriveAesKey(encryptedKey.toByteArray())
    val decryptedData = AesGcmCryptoHelper.decrypt(event.encryptedEventData, derivedKey)
    assertThat(decryptedData).isEqualTo(binaryData.toByteString())
  }
}
