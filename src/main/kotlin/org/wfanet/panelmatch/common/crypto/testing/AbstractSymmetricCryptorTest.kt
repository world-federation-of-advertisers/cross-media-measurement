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

package org.wfanet.panelmatch.common.crypto.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.panelmatch.common.crypto.SymmetricCryptor

private val PLAINTEXTS: List<ByteString> =
  listOf(
    "some plaintext0".toByteStringUtf8(),
    "some plaintext1".toByteStringUtf8(),
    "some plaintext2".toByteStringUtf8(),
    "some plaintext3".toByteStringUtf8(),
    "some plaintext4".toByteStringUtf8(),
  )

abstract class AbstractSymmetricCryptorTest {
  protected abstract val cipher: SymmetricCryptor
  protected abstract val privateKey1: ByteString
  protected abstract val privateKey2: ByteString

  @Test
  fun `generate keys should generate unique keys each time`() {
    assertThat(cipher.generateKey()).isNotEqualTo(cipher.generateKey())
  }

  @Test
  fun `encrypt result should not equal original data`() {
    assertThat(cipher.encrypt(privateKey1, PLAINTEXTS)).isNotEqualTo(PLAINTEXTS)
  }

  @Test
  fun `encrypt data and then decrypt result should equal original data`() = runBlocking {
    val encryptedValues = cipher.encrypt(privateKey1, PLAINTEXTS)
    val decryptedValues = cipher.decrypt(privateKey1, encryptedValues)
    assertThat(decryptedValues).isEqualTo(PLAINTEXTS)
  }

  @Test
  fun `encrypt data with two different keys should not be equal`() = runBlocking {
    val encryptedValues1 = cipher.encrypt(privateKey1, PLAINTEXTS)
    val encryptedValues2 = cipher.encrypt(privateKey2, PLAINTEXTS)
    assertThat(encryptedValues1).isNotEqualTo(encryptedValues2)
  }
}
