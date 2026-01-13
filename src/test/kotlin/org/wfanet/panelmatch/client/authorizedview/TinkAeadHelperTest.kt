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
import java.security.GeneralSecurityException
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class TinkAeadHelperTest {

  @Test
  fun `encrypt then decrypt returns original data`() {
    val plaintext = "test data for encryption".toByteStringUtf8()
    val key = "my-key-material-that-is-at-least-32-bytes-long".toByteStringUtf8()

    val encrypted = TinkAeadHelper.encrypt(plaintext, key)
    val decrypted = TinkAeadHelper.decrypt(encrypted, key)

    assertThat(decrypted).isEqualTo(plaintext)
  }

  @Test
  fun `encrypt with empty data works`() {
    val plaintext = ByteString.EMPTY
    val key = "a-key-that-is-at-least-32-bytes-long-for-aes".toByteStringUtf8()

    val encrypted = TinkAeadHelper.encrypt(plaintext, key)
    val decrypted = TinkAeadHelper.decrypt(encrypted, key)

    assertThat(decrypted).isEqualTo(plaintext)
  }

  @Test
  fun `encrypt with large data works`() {
    // Create a large plaintext (1MB)
    val largeData = ByteArray(1024 * 1024) { it.toByte() }
    val plaintext = largeData.toByteString()
    val key = "a-key-that-is-at-least-32-bytes-long-for-aes".toByteStringUtf8()

    val encrypted = TinkAeadHelper.encrypt(plaintext, key)
    val decrypted = TinkAeadHelper.decrypt(encrypted, key)

    assertThat(decrypted).isEqualTo(plaintext)
  }

  @Test
  fun `encrypted data differs from plaintext`() {
    val plaintext = "test data".toByteStringUtf8()
    val key = "a-key-that-is-at-least-32-bytes-long-for-aes".toByteStringUtf8()

    val encrypted = TinkAeadHelper.encrypt(plaintext, key)

    assertThat(encrypted).isNotEqualTo(plaintext)
    // Encrypted data should be longer due to IV and auth tag
    assertThat(encrypted.size()).isGreaterThan(plaintext.size())
  }

  @Test
  fun `encrypt produces different ciphertext each time due to random IV`() {
    val plaintext = "test data".toByteStringUtf8()
    val key = "a-key-that-is-at-least-32-bytes-long-for-aes".toByteStringUtf8()

    val encrypted1 = TinkAeadHelper.encrypt(plaintext, key)
    val encrypted2 = TinkAeadHelper.encrypt(plaintext, key)

    // Ciphertexts should differ due to random IV
    assertThat(encrypted1).isNotEqualTo(encrypted2)

    // But both should decrypt to the same plaintext
    assertThat(TinkAeadHelper.decrypt(encrypted1, key)).isEqualTo(plaintext)
    assertThat(TinkAeadHelper.decrypt(encrypted2, key)).isEqualTo(plaintext)
  }

  @Test
  fun `encrypt with different keys produces different ciphertext`() {
    val plaintext = "test data".toByteStringUtf8()
    val key1 = "key1-that-is-at-least-32-bytes-long-for-aes-256".toByteStringUtf8()
    val key2 = "key2-that-is-at-least-32-bytes-long-for-aes-256".toByteStringUtf8()

    val encrypted1 = TinkAeadHelper.encrypt(plaintext, key1)
    val encrypted2 = TinkAeadHelper.encrypt(plaintext, key2)

    // Both should have same length but different content
    assertThat(encrypted1.size()).isEqualTo(encrypted2.size())
    assertThat(encrypted1).isNotEqualTo(encrypted2)
  }

  @Test
  fun `decrypt with wrong key fails with GeneralSecurityException`() {
    val plaintext = "test data".toByteStringUtf8()
    val encryptionKey = "correct-key-that-is-at-least-32-bytes-long".toByteStringUtf8()
    val wrongKey = "wrong-key-that-is-also-at-least-32-bytes-long".toByteStringUtf8()

    val encrypted = TinkAeadHelper.encrypt(plaintext, encryptionKey)

    assertFailsWith<GeneralSecurityException> { TinkAeadHelper.decrypt(encrypted, wrongKey) }
  }

  @Test
  fun `decrypt with corrupted data fails`() {
    val plaintext = "test data".toByteStringUtf8()
    val key = "a-key-that-is-at-least-32-bytes-long-for-aes".toByteStringUtf8()

    val encrypted = TinkAeadHelper.encrypt(plaintext, key)

    // Corrupt the encrypted data
    val corruptedBytes = encrypted.toByteArray()
    corruptedBytes[corruptedBytes.size - 1] = (corruptedBytes[corruptedBytes.size - 1] + 1).toByte()
    val corrupted = corruptedBytes.toByteString()

    assertFailsWith<GeneralSecurityException> { TinkAeadHelper.decrypt(corrupted, key) }
  }

  @Test
  fun `decrypt with truncated data fails`() {
    val plaintext = "test data".toByteStringUtf8()
    val key = "a-key-that-is-at-least-32-bytes-long-for-aes".toByteStringUtf8()

    val encrypted = TinkAeadHelper.encrypt(plaintext, key)
    val truncated = encrypted.substring(0, 10) // Too short to be valid

    assertFailsWith<GeneralSecurityException> { TinkAeadHelper.decrypt(truncated, key) }
  }

  @Test
  fun `encrypt with key shorter than 32 bytes fails`() {
    val plaintext = "test".toByteStringUtf8()
    val invalidKey = "short-key".toByteStringUtf8() // Less than 32 bytes

    val exception =
      assertFailsWith<IllegalArgumentException> { TinkAeadHelper.encrypt(plaintext, invalidKey) }
    assertThat(exception.message).contains("Key must be at least")
  }

  @Test
  fun `decrypt with key shorter than 32 bytes fails`() {
    val plaintext = "test".toByteStringUtf8()
    val validKey = "valid-key-that-is-at-least-32-bytes-long-ok".toByteStringUtf8()
    val encrypted = TinkAeadHelper.encrypt(plaintext, validKey)

    val invalidKey = "short-key".toByteStringUtf8() // Less than 32 bytes

    val exception =
      assertFailsWith<IllegalArgumentException> { TinkAeadHelper.decrypt(encrypted, invalidKey) }
    assertThat(exception.message).contains("Key must be at least")
  }

  @Test
  fun `encrypt with exactly 32 byte key works`() {
    val plaintext = "test data".toByteStringUtf8()
    // Create exactly 32 bytes
    val key = "12345678901234567890123456789012".toByteStringUtf8() // Exactly 32 bytes

    val encrypted = TinkAeadHelper.encrypt(plaintext, key)
    val decrypted = TinkAeadHelper.decrypt(encrypted, key)

    assertThat(decrypted).isEqualTo(plaintext)
  }

  @Test
  fun `encrypt with key longer than 32 bytes uses only first 32 bytes`() {
    val plaintext = "test data".toByteStringUtf8()
    val longKey =
      "this-is-a-very-long-key-that-is-much-longer-than-32-bytes-and-will-be-truncated"
        .toByteStringUtf8()
    val truncatedKey = longKey.substring(0, 32)

    val encryptedWithLong = TinkAeadHelper.encrypt(plaintext, longKey)
    val encryptedWithTruncated = TinkAeadHelper.encrypt(plaintext, truncatedKey)

    // Can decrypt with either the long key or the truncated key (since only first 32 bytes are
    // used)
    val decryptedWithLong = TinkAeadHelper.decrypt(encryptedWithLong, longKey)
    val decryptedWithTruncated = TinkAeadHelper.decrypt(encryptedWithLong, truncatedKey)

    assertThat(decryptedWithLong).isEqualTo(plaintext)
    assertThat(decryptedWithTruncated).isEqualTo(plaintext)
  }

  @Test
  fun `encrypted data format contains IV and auth tag`() {
    val plaintext = "test".toByteStringUtf8()
    val key = "a-key-that-is-at-least-32-bytes-long-for-aes".toByteStringUtf8()

    val encrypted = TinkAeadHelper.encrypt(plaintext, key)

    // Encrypted format: IV (12 bytes) + ciphertext + auth tag (16 bytes)
    // So encrypted should be at least 12 + 16 = 28 bytes longer than plaintext
    assertThat(encrypted.size()).isAtLeast(plaintext.size() + 28)
  }

  @Test
  fun `encrypt handles binary data correctly`() {
    // Create binary data (not UTF-8 text)
    val binaryData = ByteArray(256) { it.toByte() }
    val plaintext = binaryData.toByteString()
    val key = "a-key-that-is-at-least-32-bytes-long-for-aes".toByteStringUtf8()

    val encrypted = TinkAeadHelper.encrypt(plaintext, key)
    val decrypted = TinkAeadHelper.decrypt(encrypted, key)

    assertThat(decrypted).isEqualTo(plaintext)
  }

  @Test
  fun `createAead returns working Aead instance`() {
    val key = "a-key-that-is-at-least-32-bytes-long-for-aes".toByteStringUtf8()
    val aead = TinkAeadHelper.createAead(key)

    val plaintext = "test data".toByteArray()
    val encrypted = aead.encrypt(plaintext, byteArrayOf())
    val decrypted = aead.decrypt(encrypted, byteArrayOf())

    assertThat(decrypted).isEqualTo(plaintext)
  }
}
