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
import com.google.protobuf.ByteString
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.verifyZeroInteractions
import com.nhaarman.mockitokotlin2.whenever
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.protocol.common.Cryptor
import org.wfanet.panelmatch.protocol.common.makeSerializedSharedInputs

private val KEY = ByteString.copyFromUtf8("some-key")

private val PLAINTEXTS =
  listOf<ByteString>(
    ByteString.copyFromUtf8("plaintext1"),
    ByteString.copyFromUtf8("plaintext2"),
    ByteString.copyFromUtf8("plaintext3")
  )

private val CIPHERTEXTS =
  listOf<ByteString>(
    ByteString.copyFromUtf8("ciphertext1"),
    ByteString.copyFromUtf8("ciphertext2"),
    ByteString.copyFromUtf8("ciphertext3")
  )

private val DOUBLE_CIPHERTEXTS: List<ByteString> =
  listOf<ByteString>(
    ByteString.copyFromUtf8("twice-encrypted-ciphertext1"),
    ByteString.copyFromUtf8("twice-encrypted-ciphertext2"),
    ByteString.copyFromUtf8("twice-encrypted-ciphertext3")
  )

@RunWith(JUnit4::class)
class DeterministicCommutativeCryptorExchangeTaskTest {
  private val deterministicCommutativeCryptor = mock<Cryptor>()
  private val fakeSendDebugLog: suspend (String) -> Unit = {}

  @Test
  fun `decrypt with valid inputs`() {
    whenever(deterministicCommutativeCryptor.decrypt(any(), any())).thenReturn(PLAINTEXTS)

    val result = runBlocking {
      CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encryption-key" to KEY,
            "encrypted-data" to makeSerializedSharedInputs(CIPHERTEXTS)
          ),
          fakeSendDebugLog
        )
    }

    assertThat(result).containsExactly("decrypted-data", makeSerializedSharedInputs(PLAINTEXTS))
  }

  @Test
  fun `decrypt with crypto error`() {
    whenever(deterministicCommutativeCryptor.decrypt(any(), any()))
      .thenThrow(IllegalArgumentException("Something went wrong"))

    val exception = runBlocking {
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
          .execute(
            mapOf(
              "encryption-key" to KEY,
              "encrypted-data" to makeSerializedSharedInputs(CIPHERTEXTS)
            ),
            fakeSendDebugLog
          )
      }
    }

    assertThat(exception.message).contains("Something went wrong")
  }

  @Test
  fun `decrypt with missing inputs`() {
    runBlocking {
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
          .execute(
            mapOf("encrypted-data" to makeSerializedSharedInputs(CIPHERTEXTS)),
            fakeSendDebugLog
          )
      }
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
          .execute(mapOf("encryption-key" to KEY), fakeSendDebugLog)
      }
    }

    verifyZeroInteractions(deterministicCommutativeCryptor)
  }

  @Test
  fun `encrypt with valid inputs`() {
    whenever(deterministicCommutativeCryptor.encrypt(any(), any())).thenReturn(CIPHERTEXTS)

    val result = runBlocking {
      CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encryption-key" to KEY,
            "unencrypted-data" to makeSerializedSharedInputs(PLAINTEXTS)
          ),
          fakeSendDebugLog
        )
    }

    assertThat(result).containsExactly("encrypted-data", makeSerializedSharedInputs(CIPHERTEXTS))
  }

  @Test
  fun `encrypt with crypto error`() {
    whenever(deterministicCommutativeCryptor.encrypt(any(), any()))
      .thenThrow(IllegalArgumentException("Something went wrong"))

    val exception = runBlocking {
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
          .execute(
            mapOf(
              "encryption-key" to KEY,
              "unencrypted-data" to makeSerializedSharedInputs(PLAINTEXTS)
            ),
            fakeSendDebugLog
          )
      }
    }

    assertThat(exception.message).contains("Something went wrong")
  }

  @Test
  fun `encrypt with missing inputs`() {
    runBlocking {
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
          .execute(
            mapOf("unencrypted-data" to makeSerializedSharedInputs(PLAINTEXTS)),
            fakeSendDebugLog
          )
      }
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
          .execute(mapOf("encryption-key" to KEY), fakeSendDebugLog)
      }
    }

    verifyZeroInteractions(deterministicCommutativeCryptor)
  }

  @Test
  fun `reEncryptTask with valid inputs`() {
    whenever(deterministicCommutativeCryptor.reEncrypt(any(), any())).thenReturn(DOUBLE_CIPHERTEXTS)

    val result = runBlocking {
      CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encryption-key" to KEY,
            "encrypted-data" to makeSerializedSharedInputs(CIPHERTEXTS)
          ),
          fakeSendDebugLog
        )
    }

    assertThat(result)
      .containsExactly("reencrypted-data", makeSerializedSharedInputs(DOUBLE_CIPHERTEXTS))
  }

  @Test
  fun `reEncryptTask with crypto error`() {
    whenever(deterministicCommutativeCryptor.reEncrypt(any(), any()))
      .thenThrow(IllegalArgumentException("Something went wrong"))

    val exception = runBlocking {
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
          .execute(
            mapOf(
              "encryption-key" to KEY,
              "encrypted-data" to makeSerializedSharedInputs(CIPHERTEXTS)
            ),
            fakeSendDebugLog
          )
      }
    }

    assertThat(exception.message).contains("Something went wrong")
  }

  @Test
  fun `reEncryptTask with missing inputs`() {
    runBlocking {
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
          .execute(
            mapOf("encrypted-data" to makeSerializedSharedInputs(CIPHERTEXTS)),
            fakeSendDebugLog
          )
      }
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
          .execute(mapOf("encryption-key" to KEY), fakeSendDebugLog)
      }
    }

    verifyZeroInteractions(deterministicCommutativeCryptor)
  }
}
