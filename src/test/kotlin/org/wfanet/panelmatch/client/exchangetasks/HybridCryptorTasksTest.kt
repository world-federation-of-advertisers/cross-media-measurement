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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.exchangetasks.testing.executeToByteStrings

@RunWith(JUnit4::class)
class HybridCryptorTasksTest {
  private fun createKeys(vararg inputs: Pair<String, ByteString>): Map<String, ByteString> {
    return GenerateHybridEncryptionKeyPairTask().executeToByteStrings(*inputs)
  }

  private fun encrypt(vararg inputs: Pair<String, ByteString>): Map<String, ByteString> {
    return HybridEncryptTask().executeToByteStrings(*inputs)
  }

  private fun decrypt(vararg inputs: Pair<String, ByteString>): Map<String, ByteString> {
    return HybridDecryptTask().executeToByteStrings(*inputs)
  }

  @Test
  fun privateAndPublicKeyAreDifferent() {
    val keys = createKeys()
    val privateKeyHandle = keys.getValue("private-key-handle")
    val publicKeyHandle = keys.getValue("public-key-handle")

    assertThat(privateKeyHandle).isNotEqualTo(publicKeyHandle)
  }

  @Test
  fun generatedKeysAreDifferentEachTime() {
    val keys1 = createKeys()
    val privateKeyHandle1 = keys1.getValue("private-key-handle")
    val publicKeyHandle1 = keys1.getValue("public-key-handle")

    val keys2 = createKeys()
    val privateKeyHandle2 = keys2.getValue("private-key-handle")
    val publicKeyHandle2 = keys2.getValue("public-key-handle")

    assertThat(privateKeyHandle1).isNotEqualTo(privateKeyHandle2)
    assertThat(publicKeyHandle1).isNotEqualTo(publicKeyHandle2)
  }

  @Test
  fun testGenerateKeysAndEncryptAndThenDecrypt() {
    val plaintext = "some-plaintext-data"
    val keys = createKeys()
    val privateKeyHandle = keys.getValue("private-key-handle")
    val publicKeyHandle = keys.getValue("public-key-handle")

    val encryptResults =
      encrypt(
        "public-key-handle" to publicKeyHandle,
        "plaintext-data" to plaintext.toByteStringUtf8(),
      )
    val encryptedData = encryptResults.getValue("encrypted-data")

    val decryptResults =
      decrypt("private-key-handle" to privateKeyHandle, "encrypted-data" to encryptedData)
    val decryptedData = decryptResults.getValue("decrypted-data")
    assertThat(decryptedData.toStringUtf8()).isEqualTo(plaintext)
  }

  @Test
  fun publicKeyHandleCannotDecrypt() {
    val plaintext = "some-plaintext-data"
    val keys = createKeys()
    val publicKeyHandle = keys.getValue("public-key-handle")

    val encryptResults =
      encrypt(
        "public-key-handle" to publicKeyHandle,
        "plaintext-data" to plaintext.toByteStringUtf8(),
      )
    val encryptedData = encryptResults.getValue("encrypted-data")

    assertFailsWith(java.security.GeneralSecurityException::class) {
      decrypt("private-key-handle" to publicKeyHandle, "encrypted-data" to encryptedData)
    }
  }

  @Test
  fun privateKeyHandleCannotEncrypt() {
    val plaintext = "some-plaintext-data"
    val keys = createKeys()
    val privateKeyHandle = keys.getValue("private-key-handle")

    assertFailsWith(java.security.GeneralSecurityException::class) {
      encrypt(
        "public-key-handle" to privateKeyHandle,
        "plaintext-data" to plaintext.toByteStringUtf8(),
      )
    }
  }
}
