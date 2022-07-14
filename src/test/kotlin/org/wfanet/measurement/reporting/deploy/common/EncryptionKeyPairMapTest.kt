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

package org.wfanet.measurement.reporting.deploy.common

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.nio.file.Path
import java.nio.file.Paths
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPublicKeyHandle
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.readByteString
import picocli.CommandLine
import picocli.CommandLine.Mixin

private val SECRETS_DIR: Path =
  getRuntimePath(
    Paths.get(
      "wfa_measurement_system",
      "src",
      "main",
      "k8s",
      "testing",
      "secretfiles",
    )
  )!!

private val ENCRYPTION_KEY_PAIR_MAP: Path =
  getRuntimePath(
    Paths.get(
      "wfa_measurement_system",
      "src",
      "test",
      "kotlin",
      "org",
      "wfanet",
      "measurement",
      "reporting",
      "deploy",
      "common",
      "key_pair_map.textproto"
    )
  )!!

private val PUBLIC_KEY_FILE_1 = SECRETS_DIR.resolve("mc_enc_public.tink").toFile()
private val PUBLIC_KEY_1 = PUBLIC_KEY_FILE_1.readByteString()
private val PUBLIC_KEY_FILE_2 = SECRETS_DIR.resolve("edp1_enc_public.tink").toFile()
private val PUBLIC_KEY_2 = PUBLIC_KEY_FILE_2.readByteString()
private val NON_EXISTENT_PUBLIC_KEY = "non existent public key".toByteStringUtf8()

private val PLAIN_TEXT = "THis is plain text".toByteStringUtf8()

@RunWith(JUnit4::class)
class EncryptionKeyPairMapTest {
  @Test
  fun `EncryptionKeyPairStore returns corresponding private keys`() {
    val args =
      arrayOf(
        "--key-pair-dir=$SECRETS_DIR",
        "--key-pair-config-file=$ENCRYPTION_KEY_PAIR_MAP",
      )

    runTest(args) { keyPairMap ->
      verifyKeyPair(PUBLIC_KEY_1, requireNotNull(keyPairMap[PUBLIC_KEY_1]))
      verifyKeyPair(PUBLIC_KEY_2, requireNotNull(keyPairMap[PUBLIC_KEY_2]))
    }
  }

  @Test
  fun `EncryptionKeyPairStore returns null when private key is not found`() {
    val args =
      arrayOf(
        "--key-pair-dir=$SECRETS_DIR",
        "--key-pair-config-file=$ENCRYPTION_KEY_PAIR_MAP",
      )

    runTest(args) { keyPairMap -> assertThat(keyPairMap[NON_EXISTENT_PUBLIC_KEY]).isNull() }
  }

  private class KeyPairMapWrapper(
    val verifyBlock: (keyPairs: Map<ByteString, PrivateKeyHandle>) -> Unit
  ) : Runnable {
    @Mixin lateinit var encryptionKeyPairMap: EncryptionKeyPairMap

    override fun run() {
      verifyBlock(encryptionKeyPairMap.keyPairs)
    }
  }

  private fun runTest(
    args: Array<String>,
    verifyBlock: (Map<ByteString, PrivateKeyHandle>) -> Unit
  ) {
    val returnCode = CommandLine(KeyPairMapWrapper(verifyBlock)).execute(*args)
    assertThat(returnCode).isEqualTo(0)
  }

  private fun verifyKeyPair(publicKeyData: ByteString, privateKeyHandle: PrivateKeyHandle) {
    val publicKeyHandle = TinkPublicKeyHandle(publicKeyData)
    val encryptedText = publicKeyHandle.hybridEncrypt(PLAIN_TEXT)
    val decryptedText = privateKeyHandle.hybridDecrypt(encryptedText)
    assertThat(decryptedText).isEqualTo(PLAIN_TEXT)
  }
}
