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
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verifyZeroInteractions
import com.nhaarman.mockitokotlin2.whenever
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.launcher.testing.DOUBLE_BLINDED_KEYS
import org.wfanet.panelmatch.client.launcher.testing.JOIN_KEYS
import org.wfanet.panelmatch.client.launcher.testing.LOOKUP_KEYS
import org.wfanet.panelmatch.client.launcher.testing.MP_0_SECRET_KEY
import org.wfanet.panelmatch.client.launcher.testing.SINGLE_BLINDED_KEYS
import org.wfanet.panelmatch.client.launcher.testing.buildMockCryptor
import org.wfanet.panelmatch.protocol.common.makeSerializedSharedInputs

private val MP_0_SECRET_KEY = ByteString.copyFromUtf8("some-key")

@RunWith(JUnit4::class)
class DeterministicCommutativeCryptorExchangeTaskTest {
  val deterministicCommutativeCryptor = buildMockCryptor()
  val ATTEMPT_KEY = java.util.UUID.randomUUID().toString()

  @Test
  fun `decrypt with valid inputs`() =
    runBlocking<Unit> {
      async(CoroutineName(ATTEMPT_KEY) + Dispatchers.Default) {
          val result =
            CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
              .execute(
                mapOf(
                  "encryption-key" to MP_0_SECRET_KEY,
                  "encrypted-data" to makeSerializedSharedInputs(DOUBLE_BLINDED_KEYS)
                )
              )
          assertThat(result)
            .containsExactly("decrypted-data", makeSerializedSharedInputs(LOOKUP_KEYS))
        }
        .await()
    }

  @Test
  fun `decrypt with crypto error`() =
    runBlocking<Unit> {
      whenever(deterministicCommutativeCryptor.decrypt(any(), any()))
        .thenThrow(IllegalArgumentException("Something went wrong"))

      async(CoroutineName(ATTEMPT_KEY) + Dispatchers.Default) {
          val exception =
            assertFailsWith(IllegalArgumentException::class) {
              CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
                .execute(
                  mapOf(
                    "encryption-key" to MP_0_SECRET_KEY,
                    "encrypted-data" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS)
                  )
                )
            }
          assertThat(exception.message).contains("Something went wrong")
        }
        .await()
    }

  @Test
  fun `decrypt with missing inputs`() =
    runBlocking<Unit> {
      async(CoroutineName(ATTEMPT_KEY) + Dispatchers.Default) {
          assertFailsWith(IllegalArgumentException::class) {
            CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
              .execute(mapOf("encrypted-data" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS)))
          }
          assertFailsWith(IllegalArgumentException::class) {
            CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
              .execute(mapOf("encryption-key" to MP_0_SECRET_KEY))
          }
          verifyZeroInteractions(deterministicCommutativeCryptor)
        }
        .await()
    }

  @Test
  fun `encrypt with valid inputs`() =
    runBlocking<Unit> {
      async(CoroutineName(ATTEMPT_KEY) + Dispatchers.Default) {
          val result =
            CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
              .execute(
                mapOf(
                  "encryption-key" to MP_0_SECRET_KEY,
                  "unencrypted-data" to makeSerializedSharedInputs(JOIN_KEYS)
                )
              )
          assertThat(result)
            .containsExactly("encrypted-data", makeSerializedSharedInputs(SINGLE_BLINDED_KEYS))
        }
        .await()
    }

  @Test
  fun `encrypt with crypto error`() =
    runBlocking<Unit> {
      whenever(deterministicCommutativeCryptor.encrypt(any(), any()))
        .thenThrow(IllegalArgumentException("Something went wrong"))

      async(CoroutineName(ATTEMPT_KEY) + Dispatchers.Default) {
          val exception =
            assertFailsWith(IllegalArgumentException::class) {
              CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
                .execute(
                  mapOf(
                    "encryption-key" to MP_0_SECRET_KEY,
                    "unencrypted-data" to makeSerializedSharedInputs(JOIN_KEYS)
                  )
                )
            }
          assertThat(exception.message).contains("Something went wrong")
        }
        .await()
    }

  @Test
  fun `encrypt with missing inputs`() =
    runBlocking<Unit> {
      val job =
        async(CoroutineName(ATTEMPT_KEY) + Dispatchers.Default) {
          assertFailsWith(IllegalArgumentException::class) {
            CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
              .execute(mapOf("unencrypted-data" to makeSerializedSharedInputs(JOIN_KEYS)))
          }
          assertFailsWith(IllegalArgumentException::class) {
            CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
              .execute(mapOf("encryption-key" to MP_0_SECRET_KEY))
          }
          verifyZeroInteractions(deterministicCommutativeCryptor)
        }
      job.await()
    }

  @Test
  fun `reEncryptTask with valid inputs`() =
    runBlocking<Unit> {
      whenever(deterministicCommutativeCryptor.reEncrypt(any(), any()))
        .thenReturn(DOUBLE_BLINDED_KEYS)

      async(CoroutineName(ATTEMPT_KEY) + Dispatchers.Default) {
          val result =
            CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
              .execute(
                mapOf(
                  "encryption-key" to MP_0_SECRET_KEY,
                  "encrypted-data" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS)
                )
              )
          assertThat(result)
            .containsExactly("reencrypted-data", makeSerializedSharedInputs(DOUBLE_BLINDED_KEYS))
        }
        .await()
    }

  @Test
  fun `reEncryptTask with crypto error`() =
    runBlocking<Unit> {
      whenever(deterministicCommutativeCryptor.reEncrypt(any(), any()))
        .thenThrow(IllegalArgumentException("Something went wrong"))

      async(CoroutineName(ATTEMPT_KEY) + Dispatchers.Default) {
          val exception =
            assertFailsWith(IllegalArgumentException::class) {
              CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
                .execute(
                  mapOf(
                    "encryption-key" to MP_0_SECRET_KEY,
                    "encrypted-data" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS)
                  )
                )
            }
          assertThat(exception.message).contains("Something went wrong")
        }
        .await()
    }

  @Test
  fun `reEncryptTask with missing inputs`() =
    runBlocking<Unit> {
      async(CoroutineName(ATTEMPT_KEY) + Dispatchers.Default) {
          assertFailsWith(IllegalArgumentException::class) {
            CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
              .execute(mapOf("encrypted-data" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS)))
          }
          assertFailsWith(IllegalArgumentException::class) {
            CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
              .execute(mapOf("encryption-key" to MP_0_SECRET_KEY))
          }
          verifyZeroInteractions(deterministicCommutativeCryptor)
        }
        .await()
    }
}
