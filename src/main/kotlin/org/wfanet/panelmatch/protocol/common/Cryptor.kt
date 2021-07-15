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

package org.wfanet.panelmatch.protocol.common

import com.google.protobuf.ByteString
import org.wfanet.panelmatch.protocol.CryptorDecryptRequest
import org.wfanet.panelmatch.protocol.CryptorDecryptResponse
import org.wfanet.panelmatch.protocol.CryptorEncryptRequest
import org.wfanet.panelmatch.protocol.CryptorEncryptResponse
import org.wfanet.panelmatch.protocol.CryptorReEncryptRequest
import org.wfanet.panelmatch.protocol.CryptorReEncryptResponse

/** Core deterministic, commutative cryptographic operations. */
interface Cryptor {

  /** Encrypts plaintexts. */
  fun encrypt(request: CryptorEncryptRequest): CryptorEncryptResponse

  /** Adds an additional layer of encryption to ciphertexts. */
  fun reEncrypt(request: CryptorReEncryptRequest): CryptorReEncryptResponse

  /** Removes a layer of encryption from ciphertexts. */
  fun decrypt(request: CryptorDecryptRequest): CryptorDecryptResponse

  /** Encrypts plaintexts. */
  fun encrypt(key: ByteString, plaintexts: List<ByteString>): List<ByteString> {
    val request =
      CryptorEncryptRequest.newBuilder().setEncryptionKey(key).addAllPlaintexts(plaintexts).build()
    return encrypt(request).encryptedTextsList
  }

  /** Adds an additional layer of encryption to ciphertexts. */
  fun reEncrypt(key: ByteString, encryptedTexts: List<ByteString>): List<ByteString> {
    val request =
      CryptorReEncryptRequest.newBuilder()
        .setEncryptionKey(key)
        .addAllEncryptedTexts(encryptedTexts)
        .build()
    return reEncrypt(request).reencryptedTextsList
  }

  /** Encrypts plaintexts. */
  fun decrypt(key: ByteString, encryptedTexts: List<ByteString>): List<ByteString> {
    val request =
      CryptorDecryptRequest.newBuilder()
        .setEncryptionKey(key)
        .addAllEncryptedTexts(encryptedTexts)
        .build()
    return decrypt(request).decryptedTextsList
  }
}
