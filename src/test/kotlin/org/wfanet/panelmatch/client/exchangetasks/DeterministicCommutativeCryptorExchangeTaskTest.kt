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
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.launcher.testing.JOIN_KEYS
import org.wfanet.panelmatch.common.crypto.testing.FakeDeterministicCommutativeCipher
import org.wfanet.panelmatch.common.storage.createBlob

private const val ATTEMPT_KEY = "some-arbitrary-attempt-key"

// TODO(@stevenwarejones): clean up these tests:
//   1. Only use `withContext` when we actually care about testing the specific behavior of that

@RunWith(JUnit4::class)
class DeterministicCommutativeCryptorExchangeTaskTest {
  private val mockStorage = InMemoryStorageClient()
  private val deterministicCommutativeCryptor = FakeDeterministicCommutativeCipher
  private val mpSecretKey = FakeDeterministicCommutativeCipher.generateKey()
  private val dpSecretKey = FakeDeterministicCommutativeCipher.generateKey()
  private val singleBlindedKeys = FakeDeterministicCommutativeCipher.encrypt(mpSecretKey, JOIN_KEYS)
  private val doubleBlindedKeys =
    FakeDeterministicCommutativeCipher.reEncrypt(dpSecretKey, singleBlindedKeys)
  private val lookupKeys =
    FakeDeterministicCommutativeCipher.decrypt(mpSecretKey, doubleBlindedKeys)
  private val invalidKey = FakeDeterministicCommutativeCipher.INVALID_KEY
  val hashedJoinKeysAndIds = buildJoinKeysAndIds(JOIN_KEYS)
  val singleBlindedKeysAndIds = buildJoinKeysAndIds(singleBlindedKeys)
  val doubleBlindedKeysAndIds = buildJoinKeysAndIds(doubleBlindedKeys)
  val lookupKeysAndIds = buildJoinKeysAndIds(lookupKeys)
  private val blobOfMpSecretKey = runBlocking {
    mockStorage.createBlob("mp-secret-key", mpSecretKey)
  }
  private val blobOfDpSecretKey = runBlocking {
    mockStorage.createBlob("dp-secret-key", dpSecretKey)
  }
  private val blobOfInvalidKey = runBlocking {
    mockStorage.createBlob("mp-invalid-key", invalidKey)
  }
  private val blobOfJoinKeys = runBlocking {
    mockStorage.createBlob(
      "hashed-join-keys",
      joinKeyAndIdCollection { joinKeysAndIds += hashedJoinKeysAndIds }.toByteString()
    )
  }
  private val blobOfSingleBlindedKeys = runBlocking {
    mockStorage.createBlob(
      "single-blinded-keys",
      joinKeyAndIdCollection { joinKeysAndIds += singleBlindedKeysAndIds }.toByteString()
    )
  }
  private val blobOfDoubleBlindedKeys = runBlocking {
    mockStorage.createBlob(
      "double-blinded-keys",
      joinKeyAndIdCollection { joinKeysAndIds += doubleBlindedKeysAndIds }.toByteString()
    )
  }
  @Test
  fun `decrypt with valid inputs`() = withTestContext {
    val result =
      CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
        .execute(
          mapOf("encryption-key" to blobOfMpSecretKey, "encrypted-data" to blobOfDoubleBlindedKeys)
        )
    assertThat(parseResults(result.getValue("decrypted-data").flatten()))
      .containsExactlyElementsIn(lookupKeysAndIds)
  }

  @Test
  fun `decrypt with crypto error`() = withTestContext {
    val exception =
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
          .execute(
            mapOf("encryption-key" to blobOfInvalidKey, "encrypted-data" to blobOfSingleBlindedKeys)
          )
      }
    assertThat(exception.message).contains("Invalid Key")
  }

  @Test
  fun `decrypt with missing inputs`() = withTestContext {
    assertFailsWith(NoSuchElementException::class) {
      CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
        .execute(mapOf("encrypted-data" to blobOfDoubleBlindedKeys))
    }
    assertFailsWith(NoSuchElementException::class) {
      CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
        .execute(mapOf("encryption-key" to blobOfMpSecretKey))
    }
  }

  @Test
  fun `encrypt with valid inputs`() = withTestContext {
    val result =
      CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
        .execute(mapOf("encryption-key" to blobOfMpSecretKey, "unencrypted-data" to blobOfJoinKeys))
    assertThat(parseResults(result.getValue("encrypted-data").flatten()))
      .containsExactlyElementsIn(singleBlindedKeysAndIds)
  }

  @Test
  fun `encrypt with crypto error`() = withTestContext {
    val exception =
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
          .execute(
            mapOf("encryption-key" to blobOfInvalidKey, "unencrypted-data" to blobOfJoinKeys)
          )
      }
    assertThat(exception.message).contains("Invalid Key")
  }

  @Test
  fun `encrypt with missing inputs`() = withTestContext {
    assertFailsWith(NoSuchElementException::class) {
      CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
        .execute(mapOf("unencrypted-data" to blobOfJoinKeys))
    }
    assertFailsWith(NoSuchElementException::class) {
      CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
        .execute(mapOf("encryption-key" to blobOfMpSecretKey))
    }
  }

  @Test
  fun `reEncryptTask with valid inputs`() = withTestContext {
    val result =
      CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
        .execute(
          mapOf("encryption-key" to blobOfDpSecretKey, "encrypted-data" to blobOfSingleBlindedKeys)
        )
    assertThat(parseResults(result.getValue("reencrypted-data").flatten()))
      .containsExactlyElementsIn(doubleBlindedKeysAndIds)
  }

  @Test
  fun `reEncryptTask with crypto error`() = withTestContext {
    val exception =
      assertFailsWith(IllegalArgumentException::class) {
        CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
          .execute(
            mapOf("encryption-key" to blobOfInvalidKey, "encrypted-data" to blobOfSingleBlindedKeys)
          )
      }
    assertThat(exception.message).contains("Invalid Key")
  }

  @Test
  fun `reEncryptTask with missing inputs`() = withTestContext {
    assertFailsWith(NoSuchElementException::class) {
      CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
        .execute(mapOf("encrypted-data" to blobOfSingleBlindedKeys))
    }
    assertFailsWith(NoSuchElementException::class) {
      CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
        .execute(mapOf("encryption-key" to blobOfMpSecretKey))
    }
  }
}

private fun withTestContext(block: suspend () -> Unit) {
  runBlocking { withContext(CoroutineName(ATTEMPT_KEY) + Dispatchers.Default) { block() } }
}

private fun buildJoinKeysAndIds(joinKeys: List<ByteString>): List<JoinKeyAndId> {
  return joinKeys.zip(1..joinKeys.size) { joinKeyData, keyId ->
    joinKeyAndId {
      this.joinKey = joinKey { key = joinKeyData }
      this.joinKeyIdentifier = joinKeyIdentifier { id = "joinKeyId of $keyId".toByteStringUtf8() }
    }
  }
}

private fun parseResults(cryptoResult: ByteString): List<JoinKeyAndId> {
  return JoinKeyAndIdCollection.parseFrom(cryptoResult).joinKeysAndIdsList
}
