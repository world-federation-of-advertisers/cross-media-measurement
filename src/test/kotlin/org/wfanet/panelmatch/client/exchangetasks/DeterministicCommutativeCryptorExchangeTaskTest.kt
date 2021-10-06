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
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.client.launcher.testing.JOIN_KEYS
import org.wfanet.panelmatch.client.storage.testing.makeTestVerifiedStorageClient
import org.wfanet.panelmatch.common.crypto.testing.FakeDeterministicCommutativeCipher
import org.wfanet.panelmatch.protocol.common.makeSerializedSharedInputFlow
import org.wfanet.panelmatch.protocol.common.makeSerializedSharedInputs

private const val ATTEMPT_KEY = "some-arbitrary-attempt-key"

// TODO(@stevenwarejones): clean up these tests:
//   1. Move `createBlob` calls out of the map so it's easier to see what's going on
//   2. Only use `withContext` when we actually care about testing the specific behavior of that

@RunWith(JUnit4::class)
class DeterministicCommutativeCryptorExchangeTaskTest {
  private val storage = makeTestVerifiedStorageClient()
  private val deterministicCommutativeCryptor = FakeDeterministicCommutativeCipher()
  private val mpSecretKey = FakeDeterministicCommutativeCipher().generateKey()
  private val dpSecretKey = FakeDeterministicCommutativeCipher().generateKey()
  private val singleBlindedKeys =
    FakeDeterministicCommutativeCipher().encrypt(mpSecretKey, JOIN_KEYS)
  private val doubleBlindedKeys =
    FakeDeterministicCommutativeCipher().reEncrypt(dpSecretKey, singleBlindedKeys)
  private val lookupKeys =
    FakeDeterministicCommutativeCipher().decrypt(mpSecretKey, doubleBlindedKeys)
  private val invalidKey = FakeDeterministicCommutativeCipher.INVALID_KEY

  @Test
  fun `decrypt with valid inputs`() = withTestContext {
    val result =
      CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encryption-key" to storage.createBlob("encryption-key", mpSecretKey),
            "encrypted-data" to
              storage.createBlob(
                "encrypted-data",
                makeSerializedSharedInputFlow(doubleBlindedKeys, storage.defaultBufferSizeBytes)
              )
          )
        )
        .mapValues { it.value.flatten() }
    assertThat(result).containsExactly("decrypted-data", makeSerializedSharedInputs(lookupKeys))
  }

  @Test
  fun `decrypt with crypto error`() = withTestContext {
    val exception =
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
          .execute(
            mapOf(
              "encryption-key" to storage.createBlob("encryption-key", invalidKey),
              "encrypted-data" to
                storage.createBlob(
                  "encrypted-data",
                  makeSerializedSharedInputFlow(singleBlindedKeys, storage.defaultBufferSizeBytes)
                )
            )
          )
      }
    assertThat(exception.message).contains("Invalid Key")
  }

  @Test
  fun `decrypt with missing inputs`() = withTestContext {
    assertFailsWith(NoSuchElementException::class) {
      CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encrypted-data" to
              storage.createBlob(
                "encrypted-data",
                makeSerializedSharedInputFlow(singleBlindedKeys, storage.defaultBufferSizeBytes)
              )
          )
        )
    }
    assertFailsWith(NoSuchElementException::class) {
      CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
        .execute(mapOf("encryption-key" to storage.createBlob("encryption-key", mpSecretKey)))
    }
  }

  @Test
  fun `encrypt with valid inputs`() = withTestContext {
    val result =
      CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encryption-key" to storage.createBlob("encryption-key", mpSecretKey),
            "unencrypted-data" to
              storage.createBlob(
                "unencrypted-data",
                makeSerializedSharedInputFlow(JOIN_KEYS, storage.defaultBufferSizeBytes)
              )
          )
        )
        .mapValues { it.value.flatten() }
    assertThat(result)
      .containsExactly("encrypted-data", makeSerializedSharedInputs(singleBlindedKeys))
  }

  @Test
  fun `encrypt with crypto error`() = withTestContext {
    val exception =
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
          .execute(
            mapOf(
              "encryption-key" to storage.createBlob("encryption-key", invalidKey),
              "unencrypted-data" to
                storage.createBlob(
                  "unencrypted-data",
                  makeSerializedSharedInputFlow(JOIN_KEYS, storage.defaultBufferSizeBytes)
                )
            )
          )
      }
    assertThat(exception.message).contains("Invalid Key")
  }

  @Test
  fun `encrypt with missing inputs`() = withTestContext {
    assertFailsWith(NoSuchElementException::class) {
      CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "unencrypted-data" to
              storage.createBlob(
                "unencrypted-data",
                makeSerializedSharedInputFlow(JOIN_KEYS, storage.defaultBufferSizeBytes)
              )
          )
        )
    }
    assertFailsWith(NoSuchElementException::class) {
      CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
        .execute(mapOf("encryption-key" to storage.createBlob("encryption-key", mpSecretKey)))
    }
  }

  @Test
  fun `reEncryptTask with valid inputs`() = withTestContext {
    val result: Map<String, ByteString> =
      CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encryption-key" to storage.createBlob("encryption-key", dpSecretKey),
            "encrypted-data" to
              storage.createBlob(
                "encrypted-data",
                makeSerializedSharedInputFlow(singleBlindedKeys, storage.defaultBufferSizeBytes)
              )
          )
        )
        .mapValues { it.value.flatten() }
    assertThat(result)
      .containsExactly("reencrypted-data", makeSerializedSharedInputs(doubleBlindedKeys))
  }

  @Test
  fun `reEncryptTask with crypto error`() = withTestContext {
    val exception =
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
          .execute(
            mapOf(
              "encryption-key" to storage.createBlob("encryption-key", invalidKey),
              "encrypted-data" to
                storage.createBlob(
                  "encrypted-data",
                  makeSerializedSharedInputFlow(singleBlindedKeys, storage.defaultBufferSizeBytes)
                )
            )
          )
      }
    assertThat(exception.message).contains("Invalid Key")
  }

  @Test
  fun `reEncryptTask with missing inputs`() = withTestContext {
    assertFailsWith(NoSuchElementException::class) {
      CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encrypted-data" to
              storage.createBlob(
                "encrypted-data",
                makeSerializedSharedInputFlow(singleBlindedKeys, storage.defaultBufferSizeBytes)
              )
          )
        )
    }
    assertFailsWith(NoSuchElementException::class) {
      CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
        .execute(mapOf("encryption-key" to storage.createBlob("encryption-key", mpSecretKey)))
    }
  }
}

private fun withTestContext(block: suspend () -> Unit) {
  runBlocking { withContext(CoroutineName(ATTEMPT_KEY) + Dispatchers.Default) { block() } }
}
