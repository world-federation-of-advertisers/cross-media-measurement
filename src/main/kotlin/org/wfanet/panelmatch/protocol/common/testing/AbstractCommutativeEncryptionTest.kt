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
import java.lang.RuntimeException
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import org.junit.Test
import org.wfanet.panelmatch.protocol.common.CommutativeEncryption
import wfanet.panelmatch.protocol.protobuf.ApplyCommutativeDecryptionRequest
import wfanet.panelmatch.protocol.protobuf.ApplyCommutativeEncryptionRequest
import wfanet.panelmatch.protocol.protobuf.ReApplyCommutativeEncryptionRequest

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

/** Abstract base class for testing implementations of [CommutativeEncryption]. */
abstract class AbstractCommutativeEncryptionTest {
  abstract val commutativeEncryption: CommutativeEncryption

  @Test
  fun testCommutativeEncryption() {
    val randomKey1: ByteString = ByteString.copyFromUtf8("random-key-00")
    val randomKey2: ByteString = ByteString.copyFromUtf8("random-key-222")

    val encryptedTexts1 = commutativeEncryption.encrypt(randomKey1, PLAINTEXTS)
    val encryptedTexts2 = commutativeEncryption.encrypt(randomKey2, PLAINTEXTS)
    assertThat(encryptedTexts1).isNotEqualTo(encryptedTexts2)

    val reEncryptedTexts1 = commutativeEncryption.reEncrypt(randomKey1, encryptedTexts2)
    val reEncryptedTexts2 = commutativeEncryption.reEncrypt(randomKey2, encryptedTexts1)
    assertThat(reEncryptedTexts1).isNotEqualTo(encryptedTexts2)
    assertThat(reEncryptedTexts2).isNotEqualTo(encryptedTexts1)

    val decryptedTexts1 = commutativeEncryption.decrypt(randomKey1, reEncryptedTexts1)
    val decryptedTexts2 = commutativeEncryption.decrypt(randomKey1, reEncryptedTexts2)
    assertThat(decryptedTexts1).isEqualTo(encryptedTexts2)
    assertThat(decryptedTexts2).isEqualTo(encryptedTexts2)

    val decryptedTexts3 = commutativeEncryption.decrypt(randomKey2, reEncryptedTexts1)
    val decryptedTexts4 = commutativeEncryption.decrypt(randomKey2, reEncryptedTexts2)
    assertThat(decryptedTexts3).isEqualTo(encryptedTexts1)
    assertThat(decryptedTexts4).isEqualTo(encryptedTexts1)
  }

  @Test
  fun `invalid key`() {
    val key: ByteString = ByteString.copyFromUtf8("this key is too long so it causes problems")
    assertFails { commutativeEncryption.encrypt(key, PLAINTEXTS) }
    assertFails { commutativeEncryption.decrypt(key, PLAINTEXTS) }
    assertFails { commutativeEncryption.reEncrypt(key, PLAINTEXTS) }
  }

  @Test
  fun `invalid ApplyCommutativeEncryption proto`() {
    val missingKeyException =
      assertFailsWith(RuntimeException::class) {
        val request =
          ApplyCommutativeEncryptionRequest.newBuilder().addAllPlaintexts(PLAINTEXTS).build()
        commutativeEncryption.encrypt(request)
      }
    assertThat(missingKeyException.message).contains("Failed to create the protocol cipher")
  }

  @Test
  fun `invalid ApplyCommutativeDecryption proto`() {
    val missingKeyException =
      assertFailsWith(RuntimeException::class) {
        val request =
          ApplyCommutativeDecryptionRequest.newBuilder().addAllEncryptedTexts(CIPHERTEXTS).build()
        commutativeEncryption.decrypt(request)
      }
    assertThat(missingKeyException.message).contains("Failed to create the protocol cipher")
  }

  @Test
  fun `invalid ReApplyCommutativeEncryption proto`() {
    val missingKeyException =
      assertFailsWith(RuntimeException::class) {
        val request =
          ReApplyCommutativeEncryptionRequest.newBuilder().addAllEncryptedTexts(CIPHERTEXTS).build()
        commutativeEncryption.reEncrypt(request)
      }
    assertThat(missingKeyException.message).contains("Failed to create the protocol cipher")
  }
}
