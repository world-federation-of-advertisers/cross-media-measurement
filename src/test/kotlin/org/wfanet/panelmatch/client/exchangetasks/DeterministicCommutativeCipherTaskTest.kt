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

package org.wfanet.panelmatch.client.exchangetasks

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.joinKeyAndIdCollectionOf
import org.wfanet.panelmatch.client.common.joinKeyAndIdOf
import org.wfanet.panelmatch.client.exchangetasks.testing.executeToByteStrings
import org.wfanet.panelmatch.common.crypto.testing.FakeDeterministicCommutativeCipher

private val KEY1: ByteString = FakeDeterministicCommutativeCipher.generateKey()
private val KEY2: ByteString = FakeDeterministicCommutativeCipher.generateKey()

private val PLAINTEXTS: List<ByteString> =
  listOf("some-key-1", "some-key-2").map { it.toByteStringUtf8() }
private val SINGLE_ENCRYPTED_CIPHERTEXTS: List<ByteString> =
  FakeDeterministicCommutativeCipher.encrypt(KEY1, PLAINTEXTS)
private val DOUBLE_ENCRYPTED_CIPHERTEXTS: List<ByteString> =
  FakeDeterministicCommutativeCipher.reEncrypt(KEY2, SINGLE_ENCRYPTED_CIPHERTEXTS)

private val IDS: List<ByteString> = listOf("some-id-1", "some-id-2").map { it.toByteStringUtf8() }

private val PLAINTEXT_COLLECTION =
  joinKeyAndIdCollectionOf(PLAINTEXTS.zip(IDS).map { (key, id) -> joinKeyAndIdOf(key, id) })

private val SINGLE_ENCRYPTED_CIPHERTEXT_COLLECTION =
  joinKeyAndIdCollectionOf(
    SINGLE_ENCRYPTED_CIPHERTEXTS.zip(IDS).map { (key, id) -> joinKeyAndIdOf(key, id) }
  )

private val DOUBLE_ENCRYPTED_CIPHERTEXT_COLLECTION =
  joinKeyAndIdCollectionOf(
    DOUBLE_ENCRYPTED_CIPHERTEXTS.zip(IDS).map { (key, id) -> joinKeyAndIdOf(key, id) }
  )

@RunWith(JUnit4::class)
class DeterministicCommutativeCipherTaskTest {
  private fun decrypt(vararg inputs: Pair<String, ByteString>): Map<String, ByteString> {
    return DeterministicCommutativeCipherTask.forDecryption(FakeDeterministicCommutativeCipher)
      .executeToByteStrings(*inputs)
  }

  private fun encrypt(vararg inputs: Pair<String, ByteString>): Map<String, ByteString> {
    return DeterministicCommutativeCipherTask.forEncryption(FakeDeterministicCommutativeCipher)
      .executeToByteStrings(*inputs)
  }

  private fun reEncrypt(vararg inputs: Pair<String, ByteString>): Map<String, ByteString> {
    return DeterministicCommutativeCipherTask.forReEncryption(FakeDeterministicCommutativeCipher)
      .executeToByteStrings(*inputs)
  }

  @Test
  fun valuesAreDifferent() {
    assertThat(KEY1).isNotEqualTo(KEY2)
    assertThat(PLAINTEXTS).containsNoneIn(SINGLE_ENCRYPTED_CIPHERTEXTS)
    assertThat(PLAINTEXTS).containsNoneIn(DOUBLE_ENCRYPTED_CIPHERTEXTS)
    assertThat(SINGLE_ENCRYPTED_CIPHERTEXTS).containsNoneIn(DOUBLE_ENCRYPTED_CIPHERTEXTS)
  }

  @Test
  fun decryptSingleLayerOfEncryption() {
    val result =
      decrypt(
        "encryption-key" to KEY1,
        "encrypted-data" to SINGLE_ENCRYPTED_CIPHERTEXT_COLLECTION.toByteString(),
      )
    val collection = JoinKeyAndIdCollection.parseFrom(result.getValue("decrypted-data"))
    assertThat(collection).ignoringRepeatedFieldOrder().isEqualTo(PLAINTEXT_COLLECTION)
  }

  @Test
  fun decryptTwoLayersOfEncryption() {
    val result =
      decrypt(
        "encryption-key" to KEY2,
        "encrypted-data" to DOUBLE_ENCRYPTED_CIPHERTEXT_COLLECTION.toByteString(),
      )
    val collection = JoinKeyAndIdCollection.parseFrom(result.getValue("decrypted-data"))
    assertThat(collection)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(SINGLE_ENCRYPTED_CIPHERTEXT_COLLECTION)
  }

  @Test
  fun decryptWithInvalidKey() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        decrypt(
          "encryption-key" to FakeDeterministicCommutativeCipher.INVALID_KEY,
          "encrypted-data" to SINGLE_ENCRYPTED_CIPHERTEXT_COLLECTION.toByteString(),
        )
      }
    assertThat(exception.message).contains("Invalid Key")
  }

  @Test
  fun decryptWithWrongKey() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        decrypt(
          "encryption-key" to KEY2,
          "encrypted-data" to SINGLE_ENCRYPTED_CIPHERTEXT_COLLECTION.toByteString(),
        )
      }
    assertThat(exception.message).contains("invalid ciphertext")
  }

  @Test
  fun decryptFailsWithMissingInputs() {
    assertFailsWith<NoSuchElementException> {
      decrypt("encrypted-data" to SINGLE_ENCRYPTED_CIPHERTEXT_COLLECTION.toByteString())
    }
    assertFailsWith<NoSuchElementException> { decrypt("encryption-key" to KEY1) }
  }

  @Test
  fun encryptSucceeds() {
    val result =
      encrypt("encryption-key" to KEY1, "unencrypted-data" to PLAINTEXT_COLLECTION.toByteString())
    val collection = JoinKeyAndIdCollection.parseFrom(result.getValue("encrypted-data"))
    assertThat(collection)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(SINGLE_ENCRYPTED_CIPHERTEXT_COLLECTION)
  }

  @Test
  fun encryptWithInvalidKey() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        encrypt(
          "encryption-key" to FakeDeterministicCommutativeCipher.INVALID_KEY,
          "unencrypted-data" to PLAINTEXT_COLLECTION.toByteString(),
        )
      }
    assertThat(exception.message).contains("Invalid Key")
  }

  @Test
  fun encryptFailsWithMissingInputs() {
    assertFailsWith<NoSuchElementException> {
      encrypt("unencrypted-data" to PLAINTEXT_COLLECTION.toByteString())
    }
    assertFailsWith<NoSuchElementException> { encrypt("encryption-key" to KEY1) }
  }

  @Test
  fun reencryptSucceeds() {
    val result =
      reEncrypt(
        "encryption-key" to KEY2,
        "encrypted-data" to SINGLE_ENCRYPTED_CIPHERTEXT_COLLECTION.toByteString(),
      )
    val collection = JoinKeyAndIdCollection.parseFrom(result.getValue("reencrypted-data"))
    assertThat(collection)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(DOUBLE_ENCRYPTED_CIPHERTEXT_COLLECTION)
  }

  @Test
  fun reencryptWithInvalidKey() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        reEncrypt(
          "encryption-key" to FakeDeterministicCommutativeCipher.INVALID_KEY,
          "encrypted-data" to SINGLE_ENCRYPTED_CIPHERTEXT_COLLECTION.toByteString(),
        )
      }
    assertThat(exception.message).contains("Invalid Key")
  }

  @Test
  fun reencryptFailsWithMissingInputs() {
    assertFailsWith<NoSuchElementException> {
      reEncrypt("encrypted-data" to PLAINTEXT_COLLECTION.toByteString())
    }
    assertFailsWith<NoSuchElementException> { reEncrypt("encryption-key" to KEY1) }
  }
}
