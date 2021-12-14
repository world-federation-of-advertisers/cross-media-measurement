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

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.util.UUID
import org.wfanet.panelmatch.common.crypto.DeterministicCommutativeCipher

private const val SEPARATOR = " encrypted by "
private const val FAKE_KEY_PREFIX = "fake-key-"

/** For testing only. Does not play nicely with non-Utf8 source data. */
object FakeDeterministicCommutativeCipher : DeterministicCommutativeCipher {
  val INVALID_KEY = "invalid key".toByteStringUtf8()

  override fun generateKey(): ByteString {
    return "$FAKE_KEY_PREFIX.${UUID.randomUUID()}".toByteStringUtf8()
  }

  override fun encrypt(privateKey: ByteString, plaintexts: List<ByteString>): List<ByteString> {
    require(privateKey != INVALID_KEY) { "Invalid Key" }
    return plaintexts.map { it.concat(SEPARATOR.toByteStringUtf8()).concat(privateKey) }
  }

  override fun reEncrypt(privateKey: ByteString, ciphertexts: List<ByteString>): List<ByteString> {
    require(privateKey != INVALID_KEY) { "Invalid Key" }
    return ciphertexts.map {
      require(it.toStringUtf8().contains(SEPARATOR)) { "invalid ciphertext" }
      it.concat(SEPARATOR.toByteStringUtf8()).concat(privateKey)
    }
  }

  override fun decrypt(privateKey: ByteString, ciphertexts: List<ByteString>): List<ByteString> {
    require(privateKey != INVALID_KEY) { "Invalid Key" }
    val encryptionString = SEPARATOR + privateKey.toStringUtf8()
    return ciphertexts.map {
      val dataString = it.toStringUtf8()
      require(dataString.contains(encryptionString)) { "invalid ciphertext" }
      dataString.replace(encryptionString, "").toByteStringUtf8()
    }
  }
}
