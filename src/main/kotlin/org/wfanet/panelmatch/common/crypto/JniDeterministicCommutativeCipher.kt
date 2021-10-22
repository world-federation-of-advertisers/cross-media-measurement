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

package org.wfanet.panelmatch.common.crypto

import com.google.protobuf.ByteString
import org.wfanet.panelmatch.common.loadLibraryFromResource
import org.wfanet.panelmatch.common.wrapJniException
import org.wfanet.panelmatch.protocol.CryptorDecryptResponse
import org.wfanet.panelmatch.protocol.CryptorEncryptResponse
import org.wfanet.panelmatch.protocol.CryptorGenerateKeyResponse
import org.wfanet.panelmatch.protocol.CryptorReEncryptResponse
import org.wfanet.panelmatch.protocol.crypto.DeterministicCommutativeEncryptionWrapper
import org.wfanet.panelmatch.protocol.cryptorDecryptRequest
import org.wfanet.panelmatch.protocol.cryptorEncryptRequest
import org.wfanet.panelmatch.protocol.cryptorGenerateKeyRequest
import org.wfanet.panelmatch.protocol.cryptorReEncryptRequest

private const val SWIG_PREFIX: String = "/main/swig/wfanet/panelmatch/protocol/crypto"

/**
 * A [DeterministicCommutativeCipher] implementation using the JNI
 * [DeterministicCommutativeEncryptionWrapper].
 */
class JniDeterministicCommutativeCipher : DeterministicCommutativeCipher {

  init {
    loadLibraryFromResource("deterministic_commutative_encryption", SWIG_PREFIX)
  }

  override fun generateKey(): ByteString {
    val request = cryptorGenerateKeyRequest {}
    val response = wrapJniException {
      CryptorGenerateKeyResponse.parseFrom(
        DeterministicCommutativeEncryptionWrapper.deterministicCommutativeGenerateKeyWrapper(
          request.toByteArray()
        )
      )
    }
    return response.key
  }

  override fun encrypt(privateKey: ByteString, plaintexts: List<ByteString>): List<ByteString> {
    val request = cryptorEncryptRequest {
      encryptionKey = privateKey
      this.plaintexts += plaintexts
    }
    val response = wrapJniException {
      CryptorEncryptResponse.parseFrom(
        DeterministicCommutativeEncryptionWrapper.deterministicCommutativeEncryptWrapper(
          request.toByteArray()
        )
      )
    }
    return response.ciphertextsList
  }

  override fun reEncrypt(privateKey: ByteString, ciphertexts: List<ByteString>): List<ByteString> {
    val request = cryptorReEncryptRequest {
      encryptionKey = privateKey
      this.ciphertexts += ciphertexts
    }
    val response = wrapJniException {
      CryptorReEncryptResponse.parseFrom(
        DeterministicCommutativeEncryptionWrapper.deterministicCommutativeReEncryptWrapper(
          request.toByteArray()
        )
      )
    }
    return response.ciphertextsList
  }

  override fun decrypt(privateKey: ByteString, ciphertexts: List<ByteString>): List<ByteString> {
    val request = cryptorDecryptRequest {
      encryptionKey = privateKey
      this.ciphertexts += ciphertexts
    }
    val response = wrapJniException {
      CryptorDecryptResponse.parseFrom(
        DeterministicCommutativeEncryptionWrapper.deterministicCommutativeDecryptWrapper(
          request.toByteArray()
        )
      )
    }
    return response.decryptedTextsList
  }
}
