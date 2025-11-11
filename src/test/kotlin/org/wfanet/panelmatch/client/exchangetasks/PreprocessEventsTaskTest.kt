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

import com.google.common.hash.Hashing
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFailsWith
import java.util.NoSuchElementException
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.authorizedview.TinkAeadHelper
import org.wfanet.panelmatch.client.authorizedview.DecryptedEventData
import org.wfanet.panelmatch.client.authorizedview.EncryptedMatchedEvent
import org.wfanet.panelmatch.client.authorizedview.encryptedMatchedEvent
import org.wfanet.panelmatch.client.eventpreprocessing.unprocessedEvent
import org.wfanet.panelmatch.client.exchangetasks.testing.executeToByteStrings
import org.wfanet.panelmatch.common.crypto.testing.FakeDeterministicCommutativeCipher
import org.wfanet.panelmatch.common.parseDelimitedMessages
import org.wfanet.panelmatch.common.toDelimitedByteString

@RunWith(JUnit4::class)
class PreprocessEventsTaskTest {

  private val cipher = FakeDeterministicCommutativeCipher
  private val encryptionKey = cipher.generateKey()

  private fun createTestEvents(vararg events: Pair<String, String>): ByteString {
    val outputStream = ByteString.newOutput()
    for ((id, data) in events) {
      val event = unprocessedEvent {
        this.id = id.toByteStringUtf8()
        this.data = data.toByteStringUtf8()
      }
      event.writeDelimitedTo(outputStream)
    }
    return outputStream.toByteString()
  }

  private fun executePreprocess(
    events: ByteString,
    key: ByteString = encryptionKey,
  ): Map<String, ByteString> {
    return PreprocessEventsTask(cipher)
      .executeToByteStrings("encryption-key" to key, "raw-events" to events)
  }

  private fun parseEncryptedEvents(byteString: ByteString): List<EncryptedMatchedEvent> {
    return byteString.parseDelimitedMessages(encryptedMatchedEvent {}).toList()
  }

  private fun hashKey(key: ByteString): ByteString {
    return ByteString.copyFrom(
      Hashing.sha256().hashBytes(key.toByteArray()).asBytes()
    )
  }

  @Test
  fun `successfully preprocesses single event`() {
    val events = createTestEvents("user1" to "event-data-1")

    val result = executePreprocess(events)

    assertThat(result).containsKey("encrypted-events")
    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    assertThat(encryptedEvents).hasSize(1)

    val event = encryptedEvents[0]
    // Verify join key is hashed version of encrypted key: H(G(Key))
    val expectedEncryptedKey = cipher.encrypt(encryptionKey, listOf("user1".toByteStringUtf8()))[0]
    val expectedHashedKey = hashKey(expectedEncryptedKey)
    assertThat(event.encryptedJoinKey).isEqualTo(expectedHashedKey)

    // Verify event data can be decrypted using the raw G(Key) (not derived/hashed)
    val decryptedBytes = TinkAeadHelper.decrypt(
      event.encryptedEventData,
      expectedEncryptedKey
    )
    // Parse as DecryptedEventData proto
    val decryptedEventData = DecryptedEventData.parseFrom(decryptedBytes)
    assertThat(decryptedEventData.eventDataList).hasSize(1)
    assertThat(decryptedEventData.eventDataList[0]).isEqualTo("event-data-1".toByteStringUtf8())
  }

  @Test
  fun `successfully preprocesses multiple events with different keys`() {
    val events =
      createTestEvents(
        "user1" to "event-data-1",
        "user2" to "event-data-2",
        "user3" to "event-data-3",
      )

    val result = executePreprocess(events)

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    // Each user has their own unique key, so 3 output events
    assertThat(encryptedEvents).hasSize(3)

    // Build a map from hashed key to decrypted event data for verification
    val expectedIds = listOf("user1", "user2", "user3")
    val expectedData = listOf("event-data-1", "event-data-2", "event-data-3")

    for (userId in expectedIds) {
      val expectedEncryptedKey =
        cipher.encrypt(encryptionKey, listOf(userId.toByteStringUtf8()))[0]
      val expectedHashedKey = hashKey(expectedEncryptedKey)

      // Find the event with this hashed key
      val event = encryptedEvents.find { it.encryptedJoinKey == expectedHashedKey }
      assertThat(event).isNotNull()

      // Verify data decryption using raw G(Key) (not derived/hashed)
      val decryptedBytes = TinkAeadHelper.decrypt(
        event!!.encryptedEventData,
        expectedEncryptedKey
      )
      val decryptedEventData = DecryptedEventData.parseFrom(decryptedBytes)
      assertThat(decryptedEventData.eventDataList).hasSize(1)

      val expectedIndex = expectedIds.indexOf(userId)
      assertThat(decryptedEventData.eventDataList[0])
        .isEqualTo(expectedData[expectedIndex].toByteStringUtf8())
    }
  }

  @Test
  fun `handles empty events list`() {
    val events = createTestEvents() // No events

    val result = executePreprocess(events)

    assertThat(result).containsKey("encrypted-events")
    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    assertThat(encryptedEvents).isEmpty()
  }

  @Test
  fun `fails with missing encryption key`() {
    val events = createTestEvents("user1" to "event-data")

    val exception =
      assertFailsWith<NoSuchElementException> {
        PreprocessEventsTask(cipher)
          .executeToByteStrings(
            "raw-events" to events
            // Missing "encryption-key"
          )
      }

    assertThat(exception.message).contains("encryption-key")
  }

  @Test
  fun `fails with missing raw events`() {
    val exception =
      assertFailsWith<NoSuchElementException> {
        PreprocessEventsTask(cipher)
          .executeToByteStrings(
            "encryption-key" to encryptionKey
            // Missing "raw-events"
          )
      }

    assertThat(exception.message).contains("raw-events")
  }

  @Test
  fun `produces valid EncryptedMatchedEvent protos`() {
    val events = createTestEvents("user1" to "event-data")

    val result = executePreprocess(events)

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    val event = encryptedEvents[0]

    // Verify proto structure
    assertThat(event.encryptedJoinKey.isEmpty).isFalse()
    assertThat(event.encryptedEventData.isEmpty).isFalse()

    // Encrypted data should be larger than original due to IV and auth tag
    assertThat(event.encryptedEventData.size()).isGreaterThan("event-data".length)
  }

  @Test
  fun `groups multiple events with same key into single output`() {
    val events =
      createTestEvents(
        "user1" to "event-data-1",
        "user1" to "event-data-2", // Same user ID
      )

    val result = executePreprocess(events)

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    // Same key should produce ONE output with grouped events
    assertThat(encryptedEvents).hasSize(1)

    val event = encryptedEvents[0]
    val expectedEncryptedKey = cipher.encrypt(encryptionKey, listOf("user1".toByteStringUtf8()))[0]

    // Decrypt and verify both events are grouped together
    val decryptedBytes = TinkAeadHelper.decrypt(event.encryptedEventData, expectedEncryptedKey)
    val decryptedEventData = DecryptedEventData.parseFrom(decryptedBytes)

    assertThat(decryptedEventData.eventDataList).hasSize(2)
    assertThat(decryptedEventData.eventDataList)
      .containsExactly("event-data-1".toByteStringUtf8(), "event-data-2".toByteStringUtf8())
  }

  @Test
  fun `different join keys produce different encrypted keys`() {
    val events =
      createTestEvents(
        "user1" to "event-data",
        "user2" to "event-data", // Different user ID, same data
      )

    val result = executePreprocess(events)

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    assertThat(encryptedEvents).hasSize(2)

    // Different join keys should produce different encrypted keys
    assertThat(encryptedEvents[0].encryptedJoinKey)
      .isNotEqualTo(encryptedEvents[1].encryptedJoinKey)
  }

  @Test
  fun `event data encryption uses G(Key) directly without SHA-256 hashing`() {
    val userId = "test-user"
    val eventData = "sensitive-event-data"
    val events = createTestEvents(userId to eventData)

    val result = executePreprocess(events)

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    val event = encryptedEvents[0]

    // Manually verify the encryption flow
    val gKey = cipher.encrypt(encryptionKey, listOf(userId.toByteStringUtf8()))[0]
    val hGKey = hashKey(gKey)

    // Join key should be H(G(Key))
    assertThat(event.encryptedJoinKey).isEqualTo(hGKey)

    // CRITICAL: Event data should be encrypted with raw G(Key), NOT H(G(Key))
    // This ensures only parties with G(Key) can decrypt, not those with just H(G(Key))

    // Verify decryption works with raw G(Key)
    val decryptedBytes = TinkAeadHelper.decrypt(
      event.encryptedEventData,
      gKey
    )
    val decryptedEventData = DecryptedEventData.parseFrom(decryptedBytes)
    assertThat(decryptedEventData.eventDataList).hasSize(1)
    assertThat(decryptedEventData.eventDataList[0]).isEqualTo(eventData.toByteStringUtf8())

    // CRITICAL TEST: Verify H(G(Key)) CANNOT be used to decrypt
    // This is the key security property - knowing the join key shouldn't allow decryption

    // Also verify using H(G(Key)) directly with raw decryption fails
    assertFailsWith<java.security.GeneralSecurityException> {
      TinkAeadHelper.decrypt(event.encryptedEventData, hGKey)
    }

    // Using a completely different key should also fail
    val wrongKey = "wrong-key-that-is-at-least-32-bytes-long".toByteStringUtf8()
    assertFailsWith<java.security.GeneralSecurityException> {
      TinkAeadHelper.decrypt(event.encryptedEventData, wrongKey)
    }
  }

  @Test
  fun `handles large batch processing with unique keys`() {
    // Create a request with 1000 events, each with unique key
    val eventCount = 1000
    val eventPairs = (1..eventCount).map { i -> "user$i" to "event-data-$i" }.toTypedArray()

    val events = createTestEvents(*eventPairs)

    val result = executePreprocess(events)

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    // Each user has unique key, so 1000 output events
    assertThat(encryptedEvents).hasSize(eventCount)

    // Verify first user's event
    val firstExpectedKey = cipher.encrypt(encryptionKey, listOf("user1".toByteStringUtf8()))[0]
    val firstHashedKey = hashKey(firstExpectedKey)
    val firstEvent = encryptedEvents.find { it.encryptedJoinKey == firstHashedKey }
    assertThat(firstEvent).isNotNull()

    // Verify last user's event
    val lastExpectedKey =
      cipher.encrypt(encryptionKey, listOf("user$eventCount".toByteStringUtf8()))[0]
    val lastHashedKey = hashKey(lastExpectedKey)
    val lastEvent = encryptedEvents.find { it.encryptedJoinKey == lastHashedKey }
    assertThat(lastEvent).isNotNull()
  }

  @Test
  fun `handles large batch with grouped events`() {
    // Create events where 100 users each have 10 events
    val userCount = 100
    val eventsPerUser = 10
    val eventPairs = (1..userCount).flatMap { userId ->
      (1..eventsPerUser).map { eventNum -> "user$userId" to "event-$userId-$eventNum" }
    }.toTypedArray()

    val events = createTestEvents(*eventPairs)

    val result = executePreprocess(events)

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    // 100 unique users should produce 100 output events
    assertThat(encryptedEvents).hasSize(userCount)

    // Verify one user's grouped events
    val user1Key = cipher.encrypt(encryptionKey, listOf("user1".toByteStringUtf8()))[0]
    val user1HashedKey = hashKey(user1Key)
    val user1Event = encryptedEvents.find { it.encryptedJoinKey == user1HashedKey }
    assertThat(user1Event).isNotNull()

    val decryptedBytes = TinkAeadHelper.decrypt(user1Event!!.encryptedEventData, user1Key)
    val decryptedEventData = DecryptedEventData.parseFrom(decryptedBytes)
    assertThat(decryptedEventData.eventDataList).hasSize(eventsPerUser)
  }

 @Test
  fun `throws exception for events with empty data`() {
    val events =
      createTestEvents(
        "user1" to "", // Empty event data - should throw
      )

    val exception = assertFailsWith<IllegalArgumentException> {
      executePreprocess(events)
    }
    assertThat(exception.message).contains("empty data")
  }

  @Test
  fun `handles events with binary data`() {
    // Create binary data (not UTF-8 text)
    val binaryData = ByteArray(256) { it.toByte() }
    val outputStream = ByteString.newOutput()
    val unprocessed = unprocessedEvent {
      id = "binary-user".toByteStringUtf8()
      data = binaryData.toByteString()
    }
    unprocessed.writeDelimitedTo(outputStream)
    val events = outputStream.toByteString()

    val result = executePreprocess(events)

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    val event = encryptedEvents[0]

    // Verify binary data is preserved
    val encryptedKey = cipher.encrypt(encryptionKey, listOf("binary-user".toByteStringUtf8()))[0]
    val hashedKey = hashKey(encryptedKey)
    assertThat(event.encryptedJoinKey).isEqualTo(hashedKey)

    // Verify decryption with raw G(Key) (not derived/hashed)
    val decryptedBytes = TinkAeadHelper.decrypt(
      event.encryptedEventData,
      encryptedKey
    )
    val decryptedEventData = DecryptedEventData.parseFrom(decryptedBytes)
    assertThat(decryptedEventData.eventDataList).hasSize(1)
    assertThat(decryptedEventData.eventDataList[0]).isEqualTo(binaryData.toByteString())
  }

  @Test
  fun `mixed keys with some having multiple events`() {
    val events =
      createTestEvents(
        "user1" to "event-1a",
        "user2" to "event-2a",
        "user1" to "event-1b", // Same key as first event
        "user3" to "event-3a",
        "user2" to "event-2b", // Same key as second event
      )

    val result = executePreprocess(events)

    val encryptedEvents = parseEncryptedEvents(result.getValue("encrypted-events"))
    // 3 unique users should produce 3 output events
    assertThat(encryptedEvents).hasSize(3)

    // Verify user1 has 2 events grouped
    val user1Key = cipher.encrypt(encryptionKey, listOf("user1".toByteStringUtf8()))[0]
    val user1HashedKey = hashKey(user1Key)
    val user1Event = encryptedEvents.find { it.encryptedJoinKey == user1HashedKey }
    assertThat(user1Event).isNotNull()
    val user1Decrypted = DecryptedEventData.parseFrom(
      TinkAeadHelper.decrypt(user1Event!!.encryptedEventData, user1Key)
    )
    assertThat(user1Decrypted.eventDataList).hasSize(2)
    assertThat(user1Decrypted.eventDataList)
      .containsExactly("event-1a".toByteStringUtf8(), "event-1b".toByteStringUtf8())

    // Verify user2 has 2 events grouped
    val user2Key = cipher.encrypt(encryptionKey, listOf("user2".toByteStringUtf8()))[0]
    val user2HashedKey = hashKey(user2Key)
    val user2Event = encryptedEvents.find { it.encryptedJoinKey == user2HashedKey }
    assertThat(user2Event).isNotNull()
    val user2Decrypted = DecryptedEventData.parseFrom(
      TinkAeadHelper.decrypt(user2Event!!.encryptedEventData, user2Key)
    )
    assertThat(user2Decrypted.eventDataList).hasSize(2)

    // Verify user3 has 1 event
    val user3Key = cipher.encrypt(encryptionKey, listOf("user3".toByteStringUtf8()))[0]
    val user3HashedKey = hashKey(user3Key)
    val user3Event = encryptedEvents.find { it.encryptedJoinKey == user3HashedKey }
    assertThat(user3Event).isNotNull()
    val user3Decrypted = DecryptedEventData.parseFrom(
      TinkAeadHelper.decrypt(user3Event!!.encryptedEventData, user3Key)
    )
    assertThat(user3Decrypted.eventDataList).hasSize(1)
    assertThat(user3Decrypted.eventDataList[0]).isEqualTo("event-3a".toByteStringUtf8())
  }
}