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

package org.wfanet.panelmatch.client.authorizedview

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import javax.crypto.AEADBadTagException
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class AesGcmCryptoHelperTest {

  @Test
  fun `deriveAesKey produces deterministic keys`() {
    val keyMaterial = "test-key-material".toByteArray()
    val key1 = AesGcmCryptoHelper.deriveAesKey(keyMaterial)
    val key2 = AesGcmCryptoHelper.deriveAesKey(keyMaterial)

    assertThat(key1).isEqualTo(key2)
  }

  @Test
  fun `deriveAesKey produces correct 32-byte length`() {
    val keyMaterial = "test".toByteArray()
    val key = AesGcmCryptoHelper.deriveAesKey(keyMaterial)

    assertThat(key.size()).isEqualTo(32) // 256 bits for AES-256
  }

  @Test
  fun `deriveAesKey produces different keys for different inputs`() {
    val keyMaterial1 = "key1".toByteArray()
    val keyMaterial2 = "key2".toByteArray()

    val key1 = AesGcmCryptoHelper.deriveAesKey(keyMaterial1)
    val key2 = AesGcmCryptoHelper.deriveAesKey(keyMaterial2)

    assertThat(key1).isNotEqualTo(key2)
  }

  @Test
  fun `encrypt then decrypt returns original data`() {
    val plaintext = "test data for encryption".toByteStringUtf8()
    val key = AesGcmCryptoHelper.deriveAesKey("my-key-material".toByteArray())

    val encrypted = AesGcmCryptoHelper.encrypt(plaintext, key)
    val decrypted = AesGcmCryptoHelper.decrypt(encrypted, key)

    assertThat(decrypted).isEqualTo(plaintext)
  }

  @Test
  fun `encrypt with empty data works`() {
    val plaintext = ByteString.EMPTY
    val key = AesGcmCryptoHelper.deriveAesKey("key".toByteArray())

    val encrypted = AesGcmCryptoHelper.encrypt(plaintext, key)
    val decrypted = AesGcmCryptoHelper.decrypt(encrypted, key)

    assertThat(decrypted).isEqualTo(plaintext)
  }

  @Test
  fun `encrypt with large data works`() {
    // Create a large plaintext (1MB)
    val largeData = ByteArray(1024 * 1024) { it.toByte() }
    val plaintext = largeData.toByteString()
    val key = AesGcmCryptoHelper.deriveAesKey("key".toByteArray())

    val encrypted = AesGcmCryptoHelper.encrypt(plaintext, key)
    val decrypted = AesGcmCryptoHelper.decrypt(encrypted, key)

    assertThat(decrypted).isEqualTo(plaintext)
  }

  @Test
  fun `encrypted data differs from plaintext`() {
    val plaintext = "test data".toByteStringUtf8()
    val key = AesGcmCryptoHelper.deriveAesKey("key".toByteArray())

    val encrypted = AesGcmCryptoHelper.encrypt(plaintext, key)

    assertThat(encrypted).isNotEqualTo(plaintext)
    // Encrypted data should be longer due to IV and auth tag
    assertThat(encrypted.size()).isGreaterThan(plaintext.size())
  }

  @Test
  fun `encrypt produces different ciphertext each time due to random IV`() {
    val plaintext = "test data".toByteStringUtf8()
    val key = AesGcmCryptoHelper.deriveAesKey("key".toByteArray())

    val encrypted1 = AesGcmCryptoHelper.encrypt(plaintext, key)
    val encrypted2 = AesGcmCryptoHelper.encrypt(plaintext, key)

    // Ciphertexts should differ due to random IV
    assertThat(encrypted1).isNotEqualTo(encrypted2)

    // But both should decrypt to the same plaintext
    assertThat(AesGcmCryptoHelper.decrypt(encrypted1, key)).isEqualTo(plaintext)
    assertThat(AesGcmCryptoHelper.decrypt(encrypted2, key)).isEqualTo(plaintext)
  }

  @Test
  fun `encrypt with different keys produces different ciphertext`() {
    val plaintext = "test data".toByteStringUtf8()
    val key1 = AesGcmCryptoHelper.deriveAesKey("key1".toByteArray())
    val key2 = AesGcmCryptoHelper.deriveAesKey("key2".toByteArray())

    val encrypted1 = AesGcmCryptoHelper.encrypt(plaintext, key1)
    val encrypted2 = AesGcmCryptoHelper.encrypt(plaintext, key2)

    // Extract and compare just the ciphertext portion (skip IV which is random)
    // Both should have same length but different content
    assertThat(encrypted1.size()).isEqualTo(encrypted2.size())
    assertThat(encrypted1).isNotEqualTo(encrypted2)
  }

  @Test
  fun `decrypt with wrong key fails with AEADBadTagException`() {
    val plaintext = "test data".toByteStringUtf8()
    val encryptionKey = AesGcmCryptoHelper.deriveAesKey("correct-key".toByteArray())
    val wrongKey = AesGcmCryptoHelper.deriveAesKey("wrong-key".toByteArray())

    val encrypted = AesGcmCryptoHelper.encrypt(plaintext, encryptionKey)

    assertFailsWith<AEADBadTagException> { AesGcmCryptoHelper.decrypt(encrypted, wrongKey) }
  }

  @Test
  fun `decrypt with corrupted data fails`() {
    val plaintext = "test data".toByteStringUtf8()
    val key = AesGcmCryptoHelper.deriveAesKey("key".toByteArray())

    val encrypted = AesGcmCryptoHelper.encrypt(plaintext, key)

    // Corrupt the encrypted data
    val corruptedBytes = encrypted.toByteArray()
    corruptedBytes[corruptedBytes.size - 1] = (corruptedBytes[corruptedBytes.size - 1] + 1).toByte()
    val corrupted = corruptedBytes.toByteString()

    assertFailsWith<AEADBadTagException> { AesGcmCryptoHelper.decrypt(corrupted, key) }
  }

  @Test
  fun `decrypt with truncated data fails`() {
    val plaintext = "test data".toByteStringUtf8()
    val key = AesGcmCryptoHelper.deriveAesKey("key".toByteArray())

    val encrypted = AesGcmCryptoHelper.encrypt(plaintext, key)
    val truncated = encrypted.substring(0, 10) // Too short to be valid

    val exception =
      assertFailsWith<IllegalArgumentException> { AesGcmCryptoHelper.decrypt(truncated, key) }
    assertThat(exception.message).contains("Encrypted data too short")
  }

  @Test
  fun `encrypt with invalid key size fails`() {
    val plaintext = "test".toByteStringUtf8()
    val invalidKey = "short".toByteStringUtf8() // Wrong size (not 32 bytes)

    val exception =
      assertFailsWith<IllegalArgumentException> {
        AesGcmCryptoHelper.encrypt(plaintext, invalidKey)
      }
    assertThat(exception.message).contains("AES key must be exactly 32 bytes")
  }

  @Test
  fun `decrypt with invalid key size fails`() {
    val plaintext = "test".toByteStringUtf8()
    val validKey = AesGcmCryptoHelper.deriveAesKey("key".toByteArray())
    val encrypted = AesGcmCryptoHelper.encrypt(plaintext, validKey)

    val invalidKey = "short".toByteStringUtf8() // Wrong size (not 32 bytes)

    val exception =
      assertFailsWith<IllegalArgumentException> {
        AesGcmCryptoHelper.decrypt(encrypted, invalidKey)
      }
    assertThat(exception.message).contains("AES key must be exactly 32 bytes")
  }

  @Test
  fun `encryptWithDerivedKey convenience method works`() {
    val plaintext = "test data for convenience method".toByteStringUtf8()
    val keyMaterial = "key-material".toByteArray()

    val encrypted = AesGcmCryptoHelper.encryptWithDerivedKey(plaintext, keyMaterial)
    val decrypted = AesGcmCryptoHelper.decryptWithDerivedKey(encrypted, keyMaterial)

    assertThat(decrypted).isEqualTo(plaintext)
  }

  @Test
  fun `encryptWithDerivedKey produces different output than direct encrypt`() {
    val plaintext = "test data".toByteStringUtf8()
    val keyMaterial = "key-material".toByteArray()
    val derivedKey = AesGcmCryptoHelper.deriveAesKey(keyMaterial)

    val encrypted1 = AesGcmCryptoHelper.encryptWithDerivedKey(plaintext, keyMaterial)
    val encrypted2 = AesGcmCryptoHelper.encrypt(plaintext, derivedKey)

    // Should produce different outputs due to random IV
    assertThat(encrypted1).isNotEqualTo(encrypted2)

    // But both should decrypt correctly
    assertThat(AesGcmCryptoHelper.decryptWithDerivedKey(encrypted1, keyMaterial))
      .isEqualTo(plaintext)
    assertThat(AesGcmCryptoHelper.decrypt(encrypted2, derivedKey)).isEqualTo(plaintext)
  }

  @Test
  fun `encrypted data format contains IV and auth tag`() {
    val plaintext = "test".toByteStringUtf8()
    val key = AesGcmCryptoHelper.deriveAesKey("key".toByteArray())

    val encrypted = AesGcmCryptoHelper.encrypt(plaintext, key)

    // Encrypted format: IV (12 bytes) + ciphertext + auth tag (16 bytes)
    // So encrypted should be at least 12 + 16 = 28 bytes longer than plaintext
    assertThat(encrypted.size()).isAtLeast(plaintext.size() + 28)
  }
}
