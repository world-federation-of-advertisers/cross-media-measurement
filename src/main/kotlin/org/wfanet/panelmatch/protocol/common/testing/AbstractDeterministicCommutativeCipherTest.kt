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

package org.wfanet.panelmatch.protocol.common.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertFails
import org.junit.Test
import org.wfanet.panelmatch.protocol.CryptorDecryptRequest
import org.wfanet.panelmatch.protocol.CryptorEncryptRequest
import org.wfanet.panelmatch.protocol.CryptorReEncryptRequest
import org.wfanet.panelmatch.protocol.common.DeterministicCommutativeCipher

private val PLAINTEXTS: List<ByteString> =
  listOf<ByteString>(
    ByteString.copyFromUtf8("some plaintext0"),
    ByteString.copyFromUtf8("some plaintext1"),
    ByteString.copyFromUtf8("some plaintext2"),
    ByteString.copyFromUtf8("some plaintext3"),
    ByteString.copyFromUtf8("some plaintext4")
  )

private val CIPHERTEXTS: List<ByteString> =
  listOf<ByteString>(
    ByteString.copyFromUtf8("some ciphertext0"),
    ByteString.copyFromUtf8("some ciphertext1")
  )

/** Abstract base class for testing implementations of [cipher]. */
abstract class AbstractDeterministicCommutativeCipherTest {
  abstract val cipher: DeterministicCommutativeCipher
  abstract val invalidKey: ByteString?

  @Test
  fun testCryptor() {
    val randomKey1: ByteString = ByteString.copyFromUtf8("random-key-00")
    val randomKey2: ByteString = ByteString.copyFromUtf8("random-key-222")

    val encryptedTexts1 = cipher.encrypt(randomKey1, PLAINTEXTS)
    val encryptedTexts2 = cipher.encrypt(randomKey2, PLAINTEXTS)
    assertThat(encryptedTexts1).isNotEqualTo(encryptedTexts2)

    val reEncryptedTexts1 = cipher.reEncrypt(randomKey1, encryptedTexts2)
    val reEncryptedTexts2 = cipher.reEncrypt(randomKey2, encryptedTexts1)
    assertThat(reEncryptedTexts1).isNotEqualTo(encryptedTexts2)
    assertThat(reEncryptedTexts2).isNotEqualTo(encryptedTexts1)

    val decryptedTexts1 = cipher.decrypt(randomKey1, reEncryptedTexts1)
    val decryptedTexts2 = cipher.decrypt(randomKey1, reEncryptedTexts2)
    assertThat(decryptedTexts1).isEqualTo(encryptedTexts2)
    assertThat(decryptedTexts2).isEqualTo(encryptedTexts2)

    val decryptedTexts3 = cipher.decrypt(randomKey2, reEncryptedTexts1)
    val decryptedTexts4 = cipher.decrypt(randomKey2, reEncryptedTexts2)
    assertThat(decryptedTexts3).isEqualTo(encryptedTexts1)
    assertThat(decryptedTexts4).isEqualTo(encryptedTexts1)
  }

  @Test
  fun `invalid key`() {
    val localInvalidKey = invalidKey
    if (localInvalidKey != null) {
      assertFails { cipher.encrypt(localInvalidKey, PLAINTEXTS) }
      assertFails { cipher.decrypt(localInvalidKey, PLAINTEXTS) }
      assertFails { cipher.reEncrypt(localInvalidKey, PLAINTEXTS) }
    }
  }

  @Test
  fun `invalid CryptorEncrypt proto`() {
    val request = CryptorEncryptRequest.newBuilder().addAllPlaintexts(PLAINTEXTS).build()
    assertFails { cipher.encrypt(request) }
  }

  @Test
  fun `invalid CryptorDecrypt proto`() {
    val request = CryptorDecryptRequest.newBuilder().addAllEncryptedTexts(CIPHERTEXTS).build()
    assertFails { cipher.decrypt(request) }
  }

  @Test
  fun `invalid CryptorReEncrypt proto`() {
    val request = CryptorReEncryptRequest.newBuilder().addAllEncryptedTexts(CIPHERTEXTS).build()
    assertFails { cipher.reEncrypt(request) }
  }
}
