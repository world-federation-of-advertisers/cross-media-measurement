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
import kotlinx.coroutines.flow.toList
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.authorizedview.AesGcmCryptoHelper
import org.wfanet.panelmatch.client.authorizedview.EncryptedMatchedEvent
import org.wfanet.panelmatch.client.authorizedview.encryptedMatchedEvent
import org.wfanet.panelmatch.client.exchangetasks.testing.executeToByteStrings
import org.wfanet.panelmatch.client.privatemembership.KeyedDecryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.keyedDecryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.plaintext
import org.wfanet.panelmatch.common.Fingerprinters.sha256
import org.wfanet.panelmatch.common.parseDelimitedMessages
import org.wfanet.panelmatch.common.testing.runBlockingTest
import org.wfanet.panelmatch.common.toDelimitedByteString

@RunWith(JUnit4::class)
class DecryptAndMatchEventsTaskTest {

  private fun createTestEncryptedEvent(
    gKey: ByteString,
    plaintext: ByteString,
  ): EncryptedMatchedEvent {
    val hGKey = sha256(gKey)
    val encryptedEventData = AesGcmCryptoHelper.encrypt(plaintext, gKey)
    return encryptedMatchedEvent {
      encryptedJoinKey = hGKey
      this.encryptedEventData = encryptedEventData
    }
  }

  private fun createTestMappings(
    plainKey: ByteString,
    encryptedKey: ByteString,
    identifier: ByteString,
  ): Pair<JoinKeyAndIdCollection, JoinKeyAndIdCollection> {
    val plaintextMapping = joinKeyAndIdCollection {
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = plainKey }
        joinKeyIdentifier = joinKeyIdentifier { id = identifier }
      }
    }

    val edpEncryptedMapping = joinKeyAndIdCollection {
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = encryptedKey }
        joinKeyIdentifier = joinKeyIdentifier { id = identifier }
      }
    }

    return plaintextMapping to edpEncryptedMapping
  }

  private fun parseDecryptedEvents(byteString: ByteString): List<KeyedDecryptedEventDataSet> {
    return byteString.parseDelimitedMessages(keyedDecryptedEventDataSet {}).toList()
  }

  private fun createTestInputs(
    encryptedEvents: ByteString,
    plaintextMapping: JoinKeyAndIdCollection,
    edpEncryptedMapping: JoinKeyAndIdCollection,
  ): Array<Pair<String, ByteString>> {
    return arrayOf(
      ENCRYPTED_EVENTS_LABEL to encryptedEvents,
      PLAINTEXT_JOIN_KEYS_LABEL to plaintextMapping.toByteString(),
      EDP_ENCRYPTED_KEYS_LABEL to edpEncryptedMapping.toByteString(),
    )
  }

  @Test
  fun `execute successfully decrypts and matches single event`() = runBlockingTest {
    val plainKey = TEST_PLAIN_KEY_1
    val identifier = TEST_IDENTIFIER_1
    val plaintext = TEST_PLAINTEXT_1
    val gKey = TEST_G_KEY_1

    val encryptedEvent = createTestEncryptedEvent(gKey, plaintext)
    val (plaintextMapping, edpEncryptedMapping) = createTestMappings(plainKey, gKey, identifier)

    val task = DecryptAndMatchEventsTask()
    val result =
      task.executeToByteStrings(
        *createTestInputs(
          encryptedEvent.toDelimitedByteString(),
          plaintextMapping,
          edpEncryptedMapping,
        )
      )

    val outputEvents = parseDecryptedEvents(result.getValue(OUTPUT_LABEL))

    assertThat(outputEvents).hasSize(1)
    val outputEvent = outputEvents.first()
    assertThat(outputEvent.plaintextJoinKeyAndId.joinKey.key).isEqualTo(plainKey)
    assertThat(outputEvent.plaintextJoinKeyAndId.joinKeyIdentifier.id).isEqualTo(identifier)
    assertThat(outputEvent.decryptedEventDataList).hasSize(1)
    assertThat(outputEvent.decryptedEventDataList.first().payload).isEqualTo(plaintext)
  }

  @Test
  fun `execute successfully decrypts and matches multiple events`() = runBlockingTest {
    val numEvents = 5
    val encryptedEventsBuilder = mutableListOf<ByteString>()
    val plaintextJoinKeyAndIds = mutableListOf<JoinKeyAndId>()
    val edpEncryptedJoinKeyAndIds = mutableListOf<JoinKeyAndId>()

    for (i in 1..numEvents) {
      val plainKey = "join-key-$i".toByteStringUtf8()
      val identifier = "id-$i".toByteStringUtf8()
      val plaintext = "event-data-$i".toByteStringUtf8()
      val gKey = "edp-key-$i-padded-to-32-bytes-min".toByteStringUtf8()

      val encryptedEvent = createTestEncryptedEvent(gKey, plaintext)
      encryptedEventsBuilder.add(encryptedEvent.toDelimitedByteString())

      plaintextJoinKeyAndIds.add(
        joinKeyAndId {
          joinKey = joinKey { key = plainKey }
          joinKeyIdentifier = joinKeyIdentifier { id = identifier }
        }
      )

      edpEncryptedJoinKeyAndIds.add(
        joinKeyAndId {
          joinKey = joinKey { key = gKey }
          joinKeyIdentifier = joinKeyIdentifier { id = identifier }
        }
      )
    }

    val plaintextMapping = joinKeyAndIdCollection { joinKeyAndIds += plaintextJoinKeyAndIds }
    val edpEncryptedMapping = joinKeyAndIdCollection { joinKeyAndIds += edpEncryptedJoinKeyAndIds }
    val allEncryptedEvents = ByteString.copyFrom(encryptedEventsBuilder)

    val task = DecryptAndMatchEventsTask()
    val result =
      task.executeToByteStrings(
        *createTestInputs(allEncryptedEvents, plaintextMapping, edpEncryptedMapping)
      )

    val outputEvents = parseDecryptedEvents(result.getValue(OUTPUT_LABEL))

    assertThat(outputEvents).hasSize(numEvents)
    for (i in 1..numEvents) {
      val event = outputEvents[i - 1]
      assertThat(event.plaintextJoinKeyAndId.joinKey.key)
        .isEqualTo("join-key-$i".toByteStringUtf8())
      assertThat(event.plaintextJoinKeyAndId.joinKeyIdentifier.id)
        .isEqualTo("id-$i".toByteStringUtf8())
      assertThat(event.decryptedEventDataList.first().payload)
        .isEqualTo("event-data-$i".toByteStringUtf8())
    }
  }

  @Test
  fun `execute handles binary non-UTF8 data correctly`() = runBlockingTest {
    val binaryPlainKey = BINARY_PLAIN_KEY
    val binaryIdentifier = BINARY_IDENTIFIER
    val binaryPlaintext = BINARY_PLAINTEXT

    val gKey = ByteString.copyFrom(ByteArray(32) { it.toByte() })
    val hGKey = sha256(gKey)
    val encryptedEventData = AesGcmCryptoHelper.encrypt(binaryPlaintext, gKey)

    val encryptedEvent = encryptedMatchedEvent {
      encryptedJoinKey = hGKey
      this.encryptedEventData = encryptedEventData
    }

    val (plaintextMapping, edpEncryptedMapping) =
      createTestMappings(binaryPlainKey, gKey, binaryIdentifier)

    val task = DecryptAndMatchEventsTask()
    val result =
      task.executeToByteStrings(
        *createTestInputs(
          encryptedEvent.toDelimitedByteString(),
          plaintextMapping,
          edpEncryptedMapping,
        )
      )

    val outputEvents = parseDecryptedEvents(result.getValue(OUTPUT_LABEL))

    assertThat(outputEvents).hasSize(1)
    val outputEvent = outputEvents.first()
    assertThat(outputEvent.plaintextJoinKeyAndId.joinKey.key).isEqualTo(binaryPlainKey)
    assertThat(outputEvent.plaintextJoinKeyAndId.joinKeyIdentifier.id).isEqualTo(binaryIdentifier)
    assertThat(outputEvent.decryptedEventDataList.first().payload).isEqualTo(binaryPlaintext)
  }

  @Test
  fun `execute throws on events with missing key mappings`() {
    val unmappedGKey = "unmapped-key-padded-to-32-bytes!".toByteStringUtf8()
    val encryptedEvent = createTestEncryptedEvent(unmappedGKey, "test-data".toByteStringUtf8())

    val plaintextMapping = joinKeyAndIdCollection {}
    val edpEncryptedMapping = joinKeyAndIdCollection {}

    val task = DecryptAndMatchEventsTask()
    assertFailsWith<IllegalArgumentException> {
      task.executeToByteStrings(
        *createTestInputs(
          encryptedEvent.toDelimitedByteString(),
          plaintextMapping,
          edpEncryptedMapping,
        )
      )
    }
  }

  @Test
  fun `execute throws on events with decryption failures`() {
    val plainKey = TEST_PLAIN_KEY_1
    val identifier = TEST_IDENTIFIER_1
    val gKey = TEST_G_KEY_1
    val hGKey = sha256(gKey)

    val encryptedEvent = encryptedMatchedEvent {
      encryptedJoinKey = hGKey
      encryptedEventData = "invalid-encrypted-data".toByteStringUtf8()
    }

    val (plaintextMapping, edpEncryptedMapping) = createTestMappings(plainKey, gKey, identifier)

    val task = DecryptAndMatchEventsTask()
    assertFailsWith<IllegalArgumentException> {
      task.executeToByteStrings(
        *createTestInputs(
          encryptedEvent.toDelimitedByteString(),
          plaintextMapping,
          edpEncryptedMapping,
        )
      )
    }
  }

  @Test
  fun `execute handles empty input gracefully`() = runBlockingTest {
    val plaintextMapping = joinKeyAndIdCollection {}
    val edpEncryptedMapping = joinKeyAndIdCollection {}

    val task = DecryptAndMatchEventsTask()
    val result =
      task.executeToByteStrings(
        *createTestInputs(ByteString.EMPTY, plaintextMapping, edpEncryptedMapping)
      )

    val outputEvents = parseDecryptedEvents(result.getValue(OUTPUT_LABEL))
    assertThat(outputEvents).isEmpty()
  }

  @Test
  fun `execute throws on first invalid event in mixed batch`() {
    val validPlainKey = TEST_PLAIN_KEY_1
    val validIdentifier = TEST_IDENTIFIER_1
    val validPlaintext = TEST_PLAINTEXT_1
    val validGKey = TEST_G_KEY_1
    val validEvent = createTestEncryptedEvent(validGKey, validPlaintext)

    val unmappedGKey = "unmapped-key-padded-to-32-bytes!".toByteStringUtf8()
    val unmappedEvent = createTestEncryptedEvent(unmappedGKey, "unmapped-data".toByteStringUtf8())

    val allEvents =
      ByteString.copyFrom(
        listOf(validEvent.toDelimitedByteString(), unmappedEvent.toDelimitedByteString())
      )

    val plaintextMapping = joinKeyAndIdCollection {
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = validPlainKey }
        joinKeyIdentifier = joinKeyIdentifier { id = validIdentifier }
      }
    }

    val edpEncryptedMapping = joinKeyAndIdCollection {
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = validGKey }
        joinKeyIdentifier = joinKeyIdentifier { id = validIdentifier }
      }
    }

    val task = DecryptAndMatchEventsTask()
    assertFailsWith<IllegalArgumentException> {
      task.executeToByteStrings(*createTestInputs(allEvents, plaintextMapping, edpEncryptedMapping))
    }
  }

  @Test
  fun `execute throws when missing encrypted events input`() {
    val task = DecryptAndMatchEventsTask()

    assertFailsWith<IllegalArgumentException> {
      task.executeToByteStrings(
        PLAINTEXT_JOIN_KEYS_LABEL to joinKeyAndIdCollection {}.toByteString(),
        EDP_ENCRYPTED_KEYS_LABEL to joinKeyAndIdCollection {}.toByteString(),
      )
    }
  }

  @Test
  fun `execute throws when missing plaintext join keys input`() {
    val task = DecryptAndMatchEventsTask()

    assertFailsWith<IllegalArgumentException> {
      task.executeToByteStrings(
        ENCRYPTED_EVENTS_LABEL to ByteString.EMPTY,
        EDP_ENCRYPTED_KEYS_LABEL to joinKeyAndIdCollection {}.toByteString(),
      )
    }
  }

  @Test
  fun `execute throws when missing EDP encrypted keys input`() {
    val task = DecryptAndMatchEventsTask()

    assertFailsWith<IllegalArgumentException> {
      task.executeToByteStrings(
        ENCRYPTED_EVENTS_LABEL to ByteString.EMPTY,
        PLAINTEXT_JOIN_KEYS_LABEL to joinKeyAndIdCollection {}.toByteString(),
      )
    }
  }

  @Test
  fun `execute handles duplicate identifiers in mappings correctly`() = runBlockingTest {
    val plainKey1 = "key-1".toByteStringUtf8()
    val plainKey2 = "key-2".toByteStringUtf8()
    val sharedIdentifier = TEST_IDENTIFIER_1
    val plaintext = TEST_PLAINTEXT_1

    val gKey = TEST_G_KEY_1
    val encryptedEvent = createTestEncryptedEvent(gKey, plaintext)

    val plaintextMapping = joinKeyAndIdCollection {
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = plainKey1 }
        joinKeyIdentifier = joinKeyIdentifier { id = sharedIdentifier }
      }
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = plainKey2 }
        joinKeyIdentifier = joinKeyIdentifier { id = sharedIdentifier }
      }
    }

    val edpEncryptedMapping = joinKeyAndIdCollection {
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = gKey }
        joinKeyIdentifier = joinKeyIdentifier { id = sharedIdentifier }
      }
    }

    val task = DecryptAndMatchEventsTask()
    val result =
      task.executeToByteStrings(
        *createTestInputs(
          encryptedEvent.toDelimitedByteString(),
          plaintextMapping,
          edpEncryptedMapping,
        )
      )

    val outputEvents = parseDecryptedEvents(result.getValue(OUTPUT_LABEL))

    assertThat(outputEvents).hasSize(1)
    val outputEvent = outputEvents.first()
    assertThat(outputEvent.plaintextJoinKeyAndId.joinKey.key).isEqualTo(plainKey2)
  }

  @Test
  fun `encrypt and decrypt are inverses`() = runBlockingTest {
    val key = ByteString.copyFrom(ByteArray(32) { i -> i.toByte() })
    val plaintext = "test plaintext data".toByteStringUtf8()

    val encrypted = AesGcmCryptoHelper.encrypt(plaintext, key)
    val decrypted = AesGcmCryptoHelper.decrypt(encrypted, key)

    assertThat(decrypted).isEqualTo(plaintext)
  }

  companion object {
    private val TEST_PLAIN_KEY_1 = "test-join-key-1".toByteStringUtf8()
    private val TEST_IDENTIFIER_1 = "identifier-1".toByteStringUtf8()
    private val TEST_PLAINTEXT_1 = "test-event-data".toByteStringUtf8()
    private val TEST_G_KEY_1 = "edp-encrypted-key-1-padded-to32!".toByteStringUtf8()

    private val BINARY_PLAIN_KEY =
      byteArrayOf(0x00, 0x01, 0x02, 0xFF.toByte(), 0xFE.toByte()).toByteString()
    private val BINARY_IDENTIFIER =
      byteArrayOf(0xDE.toByte(), 0xAD.toByte(), 0xBE.toByte(), 0xEF.toByte()).toByteString()
    private val BINARY_PLAINTEXT =
      byteArrayOf(0xCA.toByte(), 0xFE.toByte(), 0xBA.toByte(), 0xBE.toByte()).toByteString()

    private const val ENCRYPTED_EVENTS_LABEL = "encrypted-events"
    private const val PLAINTEXT_JOIN_KEYS_LABEL = "plaintext-join-keys"
    private const val EDP_ENCRYPTED_KEYS_LABEL = "edp-encrypted-keys"
    private const val OUTPUT_LABEL = "decrypted-event-data"
  }
}
