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
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.verifyZeroInteractions
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_CERT_PEM_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_KEY_FILE
import org.wfanet.measurement.common.crypto.testing.KEY_ALGORITHM
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.client.launcher.testing.DOUBLE_BLINDED_KEYS
import org.wfanet.panelmatch.client.launcher.testing.JOIN_KEYS
import org.wfanet.panelmatch.client.launcher.testing.LOOKUP_KEYS
import org.wfanet.panelmatch.client.launcher.testing.MP_0_SECRET_KEY
import org.wfanet.panelmatch.client.launcher.testing.SINGLE_BLINDED_KEYS
import org.wfanet.panelmatch.client.launcher.testing.buildMockCryptor
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient
import org.wfanet.panelmatch.client.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.protocol.common.makeSerializedSharedInputFlow
import org.wfanet.panelmatch.protocol.common.makeSerializedSharedInputs

private const val ATTEMPT_KEY = "some-arbitrary-attempt-key"

// TODO(@stevenwarejones): clean up these tests:
//   1. Move `createBlob` calls out of the map so it's easier to see what's going on
//   2. Only use `withContext` when we actually care about testing the specific behavior of that

@RunWith(JUnit4::class)
class DeterministicCommutativeCryptorExchangeTaskTest {
  private val mockStorage =
    VerifiedStorageClient(
      InMemoryStorageClient(keyPrefix = "mock"),
      readCertificate(FIXED_SERVER_CERT_PEM_FILE),
      readCertificate(FIXED_SERVER_CERT_PEM_FILE),
      readPrivateKey(FIXED_SERVER_KEY_FILE, KEY_ALGORITHM)
    )
  val deterministicCommutativeCryptor = buildMockCryptor()

  @Test
  fun `decrypt with valid inputs`() = withTestContext {
    val result =
      CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encryption-key" to
              mockStorage.createBlob(
                "encryption-key",
                MP_0_SECRET_KEY.asBufferedFlow(mockStorage.defaultBufferSizeBytes)
              ),
            "encrypted-data" to
              mockStorage.createBlob(
                "encrypted-data",
                makeSerializedSharedInputFlow(
                  DOUBLE_BLINDED_KEYS,
                  mockStorage.defaultBufferSizeBytes
                )
              )
          )
        )
        .mapValues { it.value.flatten() }
    assertThat(result).containsExactly("decrypted-data", makeSerializedSharedInputs(LOOKUP_KEYS))
  }

  @Test
  fun `decrypt with crypto error`() = withTestContext {
    whenever(deterministicCommutativeCryptor.decrypt(any(), any()))
      .thenThrow(IllegalArgumentException("Something went wrong"))

    val exception =
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
          .execute(
            mapOf(
              "encryption-key" to
                mockStorage.createBlob(
                  "encryption-key",
                  MP_0_SECRET_KEY.asBufferedFlow(mockStorage.defaultBufferSizeBytes)
                ),
              "encrypted-data" to
                mockStorage.createBlob(
                  "encrypted-data",
                  makeSerializedSharedInputFlow(
                    SINGLE_BLINDED_KEYS,
                    mockStorage.defaultBufferSizeBytes
                  )
                )
            )
          )
      }
    assertThat(exception.message).contains("Something went wrong")
  }

  @Test
  fun `decrypt with missing inputs`() = withTestContext {
    assertFailsWith(IllegalArgumentException::class) {
      CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encrypted-data" to
              mockStorage.createBlob(
                "encrypted-data",
                makeSerializedSharedInputFlow(
                  SINGLE_BLINDED_KEYS,
                  mockStorage.defaultBufferSizeBytes
                )
              )
          )
        )
    }
    assertFailsWith(IllegalArgumentException::class) {
      CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encryption-key" to
              mockStorage.createBlob(
                "encryption-key",
                MP_0_SECRET_KEY.asBufferedFlow(mockStorage.defaultBufferSizeBytes)
              )
          )
        )
    }
    verifyZeroInteractions(deterministicCommutativeCryptor)
  }

  @Test
  fun `encrypt with valid inputs`() = withTestContext {
    val result =
      CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encryption-key" to
              mockStorage.createBlob(
                "encryption-key",
                MP_0_SECRET_KEY.asBufferedFlow(mockStorage.defaultBufferSizeBytes)
              ),
            "unencrypted-data" to
              mockStorage.createBlob(
                "unencrypted-data",
                makeSerializedSharedInputFlow(JOIN_KEYS, mockStorage.defaultBufferSizeBytes)
              )
          )
        )
        .mapValues { it.value.flatten() }
    assertThat(result)
      .containsExactly("encrypted-data", makeSerializedSharedInputs(SINGLE_BLINDED_KEYS))
  }

  @Test
  fun `encrypt with crypto error`() = withTestContext {
    whenever(deterministicCommutativeCryptor.encrypt(any(), any()))
      .thenThrow(IllegalArgumentException("Something went wrong"))

    val exception =
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
          .execute(
            mapOf(
              "encryption-key" to
                mockStorage.createBlob(
                  "encryption-key",
                  MP_0_SECRET_KEY.asBufferedFlow(mockStorage.defaultBufferSizeBytes)
                ),
              "unencrypted-data" to
                mockStorage.createBlob(
                  "unencrypted-data",
                  makeSerializedSharedInputFlow(JOIN_KEYS, mockStorage.defaultBufferSizeBytes)
                )
            )
          )
      }
    assertThat(exception.message).contains("Something went wrong")
  }

  @Test
  fun `encrypt with missing inputs`() = withTestContext {
    assertFailsWith(IllegalArgumentException::class) {
      CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "unencrypted-data" to
              mockStorage.createBlob(
                "unencrypted-data",
                makeSerializedSharedInputFlow(JOIN_KEYS, mockStorage.defaultBufferSizeBytes)
              )
          )
        )
    }
    assertFailsWith(IllegalArgumentException::class) {
      CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encryption-key" to
              mockStorage.createBlob(
                "encryption-key",
                MP_0_SECRET_KEY.asBufferedFlow(mockStorage.defaultBufferSizeBytes)
              )
          )
        )
    }
    verifyZeroInteractions(deterministicCommutativeCryptor)
  }

  @Test
  fun `reEncryptTask with valid inputs`() = withTestContext {
    whenever(deterministicCommutativeCryptor.reEncrypt(any(), any()))
      .thenReturn(DOUBLE_BLINDED_KEYS)

    val result =
      CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encryption-key" to
              mockStorage.createBlob(
                "encryption-key",
                MP_0_SECRET_KEY.asBufferedFlow(mockStorage.defaultBufferSizeBytes)
              ),
            "encrypted-data" to
              mockStorage.createBlob(
                "encrypted-data",
                makeSerializedSharedInputFlow(
                  SINGLE_BLINDED_KEYS,
                  mockStorage.defaultBufferSizeBytes
                )
              )
          )
        )
        .mapValues { it.value.fold(ByteString.EMPTY, { agg, chunk -> agg.concat(chunk) }) }
    assertThat(result)
      .containsExactly("reencrypted-data", makeSerializedSharedInputs(DOUBLE_BLINDED_KEYS))
  }

  @Test
  fun `reEncryptTask with crypto error`() = withTestContext {
    whenever(deterministicCommutativeCryptor.reEncrypt(any(), any()))
      .thenThrow(IllegalArgumentException("Something went wrong"))

    val exception =
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
          .execute(
            mapOf(
              "encryption-key" to
                mockStorage.createBlob(
                  "encryption-key",
                  MP_0_SECRET_KEY.asBufferedFlow(mockStorage.defaultBufferSizeBytes)
                ),
              "encrypted-data" to
                mockStorage.createBlob(
                  "encrypted-data",
                  makeSerializedSharedInputFlow(
                    SINGLE_BLINDED_KEYS,
                    mockStorage.defaultBufferSizeBytes
                  )
                )
            )
          )
      }
    assertThat(exception.message).contains("Something went wrong")
  }

  @Test
  fun `reEncryptTask with missing inputs`() = withTestContext {
    assertFailsWith(IllegalArgumentException::class) {
      CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encrypted-data" to
              mockStorage.createBlob(
                "encrypted-data",
                makeSerializedSharedInputFlow(
                  SINGLE_BLINDED_KEYS,
                  mockStorage.defaultBufferSizeBytes
                )
              )
          )
        )
    }
    assertFailsWith(IllegalArgumentException::class) {
      CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
        .execute(
          mapOf(
            "encryption-key" to
              mockStorage.createBlob(
                "encryption-key",
                MP_0_SECRET_KEY.asBufferedFlow(mockStorage.defaultBufferSizeBytes)
              )
          )
        )
    }
    verifyZeroInteractions(deterministicCommutativeCryptor)
  }
}

private fun withTestContext(block: suspend () -> Unit) {
  runBlocking { withContext(CoroutineName(ATTEMPT_KEY) + Dispatchers.Default) { block() } }
}
