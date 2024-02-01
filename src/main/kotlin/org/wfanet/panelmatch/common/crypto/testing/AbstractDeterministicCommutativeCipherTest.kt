// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.common.crypto.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFails
import org.junit.Test
import org.wfanet.panelmatch.common.crypto.DeterministicCommutativeCipher

private val PLAINTEXTS: List<ByteString> =
  listOf(
    "some plaintext0".toByteStringUtf8(),
    "some plaintext1".toByteStringUtf8(),
    "some plaintext2".toByteStringUtf8(),
    "some plaintext3".toByteStringUtf8(),
    "some plaintext4".toByteStringUtf8(),
  )

private val CIPHERTEXTS: List<ByteString> =
  listOf("some ciphertext0".toByteStringUtf8(), "some ciphertext1".toByteStringUtf8())

/** Abstract base class for testing implementations of [cipher]. */
abstract class AbstractDeterministicCommutativeCipherTest {
  abstract val cipher: DeterministicCommutativeCipher
  abstract val privateKey1: ByteString
  abstract val privateKey2: ByteString
  abstract val invalidKey: ByteString

  @Test
  fun testCryptor() {

    val encryptedTexts1 = cipher.encrypt(privateKey1, PLAINTEXTS)
    val encryptedTexts2 = cipher.encrypt(privateKey2, PLAINTEXTS)
    assertThat(encryptedTexts1).isNotEqualTo(encryptedTexts2)

    val reEncryptedTexts1 = cipher.reEncrypt(privateKey1, encryptedTexts2)
    val reEncryptedTexts2 = cipher.reEncrypt(privateKey2, encryptedTexts1)
    assertThat(reEncryptedTexts1).isNotEqualTo(encryptedTexts2)
    assertThat(reEncryptedTexts2).isNotEqualTo(encryptedTexts1)

    val decryptedTexts1 = cipher.decrypt(privateKey1, reEncryptedTexts1)
    val decryptedTexts2 = cipher.decrypt(privateKey1, reEncryptedTexts2)
    assertThat(decryptedTexts1).isEqualTo(encryptedTexts2)
    assertThat(decryptedTexts2).isEqualTo(encryptedTexts2)

    val decryptedTexts3 = cipher.decrypt(privateKey2, reEncryptedTexts1)
    val decryptedTexts4 = cipher.decrypt(privateKey2, reEncryptedTexts2)
    assertThat(decryptedTexts3).isEqualTo(encryptedTexts1)
    assertThat(decryptedTexts4).isEqualTo(encryptedTexts1)
  }

  @Test
  fun `invalid key`() {
    val localInvalidKey = invalidKey
    assertFails { cipher.encrypt(localInvalidKey, PLAINTEXTS) }
    assertFails { cipher.decrypt(localInvalidKey, PLAINTEXTS) }
    assertFails { cipher.reEncrypt(localInvalidKey, PLAINTEXTS) }
  }

  @Test
  fun `invalid ciphertexts`() {
    val key: ByteString = cipher.generateKey()
    assertFails { cipher.decrypt(key, CIPHERTEXTS) }
    assertFails { cipher.reEncrypt(key, CIPHERTEXTS) }
  }
}
