// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.proto.Keyset
import com.google.protobuf.ByteString
import com.google.protobuf.CodedOutputStream
import com.google.protobuf.kotlin.toByteStringUtf8
import java.io.ByteArrayOutputStream
import java.nio.file.Path
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPublicKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.readByteString

private val SECRETS_DIR: Path =
  getRuntimePath(
    Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
  )!!

private val PUBLIC_KEY_FILE_1 = SECRETS_DIR.resolve("mc_enc_public.tink").toFile()
private val PUBLIC_KEY_1 = PUBLIC_KEY_FILE_1.readByteString()
private val PRIVATE_KEY_FILE_1 = SECRETS_DIR.resolve("mc_enc_private.tink").toFile()
private val PRIVATE_KEY_HANDLE_1 = loadPrivateKey(PRIVATE_KEY_FILE_1)
private val PUBLIC_KEY_FILE_2 = SECRETS_DIR.resolve("edp1_enc_public.tink").toFile()
private val PUBLIC_KEY_2 = PUBLIC_KEY_FILE_2.readByteString()
private val PRIVATE_KEY_FILE_2 = SECRETS_DIR.resolve("edp1_enc_private.tink").toFile()
private val PRIVATE_KEY_HANDLE_2 = loadPrivateKey(PRIVATE_KEY_FILE_2)
private val NON_EXISTENT_PUBLIC_KEY = "non existent public key".toByteStringUtf8()
private val PRINCIPAL_NAME = "measurement_consumer1"
private val NON_EXISTENT_PRINCIPAL_NAME = "measurement_consumer2"
private val PLAIN_TEXT = "THis is plain text".toByteStringUtf8()

@RunWith(JUnit4::class)
class EncryptionKeyPairStoreTest {
  @Test
  fun `InMemoryEncryptionKeyPairStore returns PrivateKeyHandle for existing public key`() {
    val keyPairStore = InMemoryEncryptionKeyPairStore(getKeyPairs())

    verifyKeyPair(keyPairStore, PRINCIPAL_NAME, PUBLIC_KEY_1)
    verifyKeyPair(keyPairStore, PRINCIPAL_NAME, PUBLIC_KEY_2)
  }

  @Test
  fun `InMemoryEncryptionKeyPairStore returns null for non-existing public key`() {
    val keyPairStore = InMemoryEncryptionKeyPairStore(getKeyPairs())

    assertThat(
        runBlocking { keyPairStore.getPrivateKeyHandle(PRINCIPAL_NAME, NON_EXISTENT_PUBLIC_KEY) }
      )
      .isNull()
  }

  @Test
  fun `InMemoryEncryptionKeyPairStore returns null for non-existing principal`() {
    val keyPairStore = InMemoryEncryptionKeyPairStore(getKeyPairs())

    assertThat(
        runBlocking { keyPairStore.getPrivateKeyHandle(NON_EXISTENT_PRINCIPAL_NAME, PUBLIC_KEY_1) }
      )
      .isNull()
  }

  @Test
  fun `InMemoryEncryptionKeyPairStore returns PrivateKeyHandle for a different serialization of the same public key`() {
    val keyPairStore = InMemoryEncryptionKeyPairStore(getKeyPairs())

    verifyKeyPair(keyPairStore, PRINCIPAL_NAME, reserialize(PUBLIC_KEY_1))
  }

  private fun getKeyPairs(): Map<String, List<Pair<ByteString, PrivateKeyHandle>>> {
    return mapOf(
      PRINCIPAL_NAME to
        listOf(PUBLIC_KEY_1 to PRIVATE_KEY_HANDLE_1, PUBLIC_KEY_2 to PRIVATE_KEY_HANDLE_2)
    )
  }

  /**
   * Returns a serialization of [publicKey]'s underlying keyset that differs, byte for byte, from
   * [publicKey] while still parsing to the same key.
   *
   * [Keyset] has exactly two fields -- `primary_key_id` (1) and `key` (2) -- and the standard
   * generated writer always emits them in that, ascending, field-number order. Writing them in the
   * opposite order by hand produces a wire-format-valid encoding of the identical, fully-recognized
   * content that no field-number-ordered writer would ever produce -- the kind of byte-level
   * difference a different serializer implementation (e.g. a different Tink version) could
   * introduce for the same logical key, as opposed to an artificial unknown field.
   */
  private fun reserialize(publicKey: ByteString): ByteString {
    val keyset = Keyset.parseFrom(publicKey)
    val outputStream = ByteArrayOutputStream()
    val codedOutput = CodedOutputStream.newInstance(outputStream)
    for (key in keyset.keyList) {
      codedOutput.writeMessage(2, key)
    }
    codedOutput.writeUInt32(1, keyset.primaryKeyId)
    codedOutput.flush()
    val reserialized = ByteString.copyFrom(outputStream.toByteArray())

    check(reserialized != publicKey) {
      "Expected a different serialization for this test to be meaningful"
    }
    check(Keyset.parseFrom(reserialized) == keyset) {
      "Expected the reordered serialization to parse back to an identical Keyset"
    }
    return reserialized
  }

  private fun verifyKeyPair(
    keyPairStore: InMemoryEncryptionKeyPairStore,
    principalName: String,
    publicKey: ByteString,
  ) {
    val privateKeyHandle = runBlocking {
      requireNotNull(keyPairStore.getPrivateKeyHandle(principalName, publicKey))
    }
    val publicKeyHandle = TinkPublicKeyHandle(publicKey)
    val encryptedText = publicKeyHandle.hybridEncrypt(PLAIN_TEXT)
    val decryptedText = privateKeyHandle.hybridDecrypt(encryptedText)
    assertThat(decryptedText).isEqualTo(PLAIN_TEXT)
  }
}
