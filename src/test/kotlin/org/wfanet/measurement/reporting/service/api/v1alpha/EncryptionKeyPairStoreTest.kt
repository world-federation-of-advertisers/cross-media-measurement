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

package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import java.nio.file.Path
import java.nio.file.Paths
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
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

private val PUBLIC_KEY_FILE_1 = SECRETS_DIR.resolve("mc_enc_public.tink").toFile()
private val PUBLIC_KEY_1 = PUBLIC_KEY_FILE_1.readByteString()
private val PRIVATE_KEY_FILE_1 = SECRETS_DIR.resolve("mc_enc_private.tink").toFile()
private val PUBLIC_KEY_FILE_2 = SECRETS_DIR.resolve("edp1_enc_public.tink").toFile()
private val PUBLIC_KEY_2 = PUBLIC_KEY_FILE_2.readByteString()
private val PRIVATE_KEY_FILE_2 = SECRETS_DIR.resolve("edp2_enc_private.tink").toFile()
private val NON_EXISTENT_PUBLIC_KEY = "non existent public key".toByteStringUtf8()

@RunWith(JUnit4::class)
class EncryptionKeyPairStoreTest {
  private class PairStoreWrapper(val verifyBlock: (InMemoryEncryptionKeyPairStore) -> Unit) :
    Runnable {
    @Mixin lateinit var encryptionKeyPairStore: InMemoryEncryptionKeyPairStore

    override fun run() {
      verifyBlock(encryptionKeyPairStore)
    }
  }

  private fun runTest(args: Array<String>, verifyBlock: (InMemoryEncryptionKeyPairStore) -> Unit) {
    val returnCode = CommandLine(PairStoreWrapper(verifyBlock)).execute(*args)
    assertThat(returnCode).isEqualTo(0)
  }

  @Test
  fun `EncryptionKeyPairStore returns corresponding private keys`() {
    val args =
      arrayOf(
        "--encryption-public-key-file=$PUBLIC_KEY_FILE_1",
        "--encryption-private-key-file=$PRIVATE_KEY_FILE_1",
        "--encryption-public-key-file=$PUBLIC_KEY_FILE_2",
        "--encryption-private-key-file=$PRIVATE_KEY_FILE_2",
      )

    runTest(args) { keyPairStore ->
      assertThat(keyPairStore.getPrivateKey(PUBLIC_KEY_1)).isNotNull()
      assertThat(keyPairStore.getPrivateKey(PUBLIC_KEY_2)).isNotNull()
    }
  }

  @Test
  fun `EncryptionKeyPairStore returns null when private key is not found`() {
    val args =
      arrayOf(
        "--encryption-public-key-file=$PUBLIC_KEY_FILE_1",
        "--encryption-private-key-file=$PRIVATE_KEY_FILE_1",
        "--encryption-public-key-file=$PUBLIC_KEY_FILE_2",
        "--encryption-private-key-file=$PRIVATE_KEY_FILE_2",
      )

    runTest(args) { keyPairStore ->
      assertThat(keyPairStore.getPrivateKey(NON_EXISTENT_PUBLIC_KEY)).isNull()
    }
  }
}
