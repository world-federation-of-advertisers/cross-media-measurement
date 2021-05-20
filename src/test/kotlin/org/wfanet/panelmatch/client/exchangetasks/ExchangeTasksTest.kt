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
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.protocol.common.applyCommutativeDecryption
import org.wfanet.panelmatch.protocol.common.applyCommutativeEncryption
import org.wfanet.panelmatch.protocol.common.reApplyCommutativeEncryption
import wfanet.panelmatch.protocol.protobuf.SharedInputs

@RunWith(JUnit4::class)
class ExchangeTasksTest {
  private val joinKeys =
    listOf<ByteString>(
      ByteString.copyFromUtf8("some key0"),
      ByteString.copyFromUtf8("some key1"),
      ByteString.copyFromUtf8("some key2"),
      ByteString.copyFromUtf8("some key3"),
      ByteString.copyFromUtf8("some key4")
    )
  private val randomCommutativeDeterministicKey1: ByteString =
    ByteString.copyFromUtf8("random-key-000")
  private val randomCommutativeDeterministicKey2: ByteString =
    ByteString.copyFromUtf8("random-key-222")

  @Test
  fun `test encrypt reencrypt and decrypt tasks`() = runBlocking {
    val fakeSendDebugLog: suspend (String) -> Unit = {}

    val encryptedOutputs: Map<String, ByteString> =
      EncryptTask()
        .execute(
          mapOf(
            "encryption-key" to randomCommutativeDeterministicKey1,
            "unencrypted-data" to
              SharedInputs.newBuilder().addAllData(joinKeys).build().toByteString()
          ),
          fakeSendDebugLog
        )
    assertThat(SharedInputs.parseFrom(encryptedOutputs["encrypted-data"]).getDataList())
      .isEqualTo(applyCommutativeEncryption(randomCommutativeDeterministicKey1, joinKeys))

    val reEncryptedOutputs: Map<String, ByteString> =
      ReEncryptTask()
        .execute(
          mapOf(
            "encryption-key" to randomCommutativeDeterministicKey2,
            "encrypted-data" to encryptedOutputs["encrypted-data"]!!
          ),
          fakeSendDebugLog
        )
    assertThat(SharedInputs.parseFrom(reEncryptedOutputs["reencrypted-data"]).getDataList())
      .isEqualTo(
        reApplyCommutativeEncryption(
          randomCommutativeDeterministicKey2,
          applyCommutativeEncryption(randomCommutativeDeterministicKey1, joinKeys)
        )
      )

    val decryptedOutputs: Map<String, ByteString> =
      DecryptTask()
        .execute(
          mapOf(
            "encryption-key" to randomCommutativeDeterministicKey1,
            "encrypted-data" to reEncryptedOutputs["reencrypted-data"]!!
          ),
          fakeSendDebugLog
        )
    assertThat(SharedInputs.parseFrom(decryptedOutputs["decrypted-data"]).getDataList())
      .isEqualTo(
        applyCommutativeDecryption(
          randomCommutativeDeterministicKey1,
          reApplyCommutativeEncryption(
            randomCommutativeDeterministicKey2,
            applyCommutativeEncryption(randomCommutativeDeterministicKey1, joinKeys)
          )
        )
      )
  }

  @Test
  fun detectErrorsfromJni() = runBlocking {
    val randomCommutativeDeterministicKeyInvalid: ByteString = ByteString.copyFromUtf8("")
    val fakeSendDebugLog: suspend (String) -> Unit = {}
    val decryptException =
      assertFailsWith(RuntimeException::class) {
        DecryptTask()
          .execute(
            mapOf(
              "encryption-key" to randomCommutativeDeterministicKeyInvalid,
              "encrypted-data" to
                SharedInputs.newBuilder().addAllData(joinKeys).build().toByteString()
            ),
            fakeSendDebugLog
          )
      }
    assertThat(decryptException.message)
      .contains("INVALID_ARGUMENT: Failed to create the protocol cipher")

    val encryptException =
      assertFailsWith(RuntimeException::class) {
        EncryptTask()
          .execute(
            mapOf(
              "encryption-key" to randomCommutativeDeterministicKeyInvalid,
              "unencrypted-data" to
                SharedInputs.newBuilder().addAllData(joinKeys).build().toByteString()
            ),
            fakeSendDebugLog
          )
      }
    assertThat(encryptException.message)
      .contains("INVALID_ARGUMENT: Failed to create the protocol cipher")

    val reencryptException =
      assertFailsWith(RuntimeException::class) {
        ReEncryptTask()
          .execute(
            mapOf(
              "encryption-key" to randomCommutativeDeterministicKeyInvalid,
              "encrypted-data" to
                SharedInputs.newBuilder().addAllData(joinKeys).build().toByteString()
            ),
            fakeSendDebugLog
          )
      }
    assertThat(reencryptException.message)
      .contains("INVALID_ARGUMENT: Failed to create the protocol cipher")
  }

  @Test
  fun detectMissingTaskFields() = runBlocking {
    val fakeSendDebugLog: suspend (String) -> Unit = {}
    assertFailsWith<IllegalArgumentException> {
      DecryptTask()
        .execute(
          mapOf(
            "encrypted-data" to
              SharedInputs.newBuilder().addAllData(joinKeys).build().toByteString()
          ),
          fakeSendDebugLog
        )
    }
    val encryptException =
      assertFailsWith(IllegalArgumentException::class) {
        EncryptTask()
          .execute(
            mapOf(
              "unencrypted-data" to
                SharedInputs.newBuilder().addAllData(joinKeys).build().toByteString()
            ),
            fakeSendDebugLog
          )
      }
    val reencryptException =
      assertFailsWith(IllegalArgumentException::class) {
        ReEncryptTask()
          .execute(
            mapOf(
              "encrypted-data" to
                SharedInputs.newBuilder().addAllData(joinKeys).build().toByteString()
            ),
            fakeSendDebugLog
          )
      }
  }
}
