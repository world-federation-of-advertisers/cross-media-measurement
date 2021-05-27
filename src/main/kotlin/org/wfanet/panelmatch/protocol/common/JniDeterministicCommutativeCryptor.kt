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

import java.lang.RuntimeException
import java.nio.file.Paths
import org.wfanet.panelmatch.common.loadLibrary
import wfanet.panelmatch.protocol.crypto.DeterministicCommutativeEncryptionUtility
import wfanet.panelmatch.protocol.protobuf.CryptorDecryptRequest
import wfanet.panelmatch.protocol.protobuf.CryptorDecryptResponse
import wfanet.panelmatch.protocol.protobuf.CryptorEncryptRequest
import wfanet.panelmatch.protocol.protobuf.CryptorEncryptResponse
import wfanet.panelmatch.protocol.protobuf.CryptorReEncryptRequest
import wfanet.panelmatch.protocol.protobuf.CryptorReEncryptResponse

/**
 * A [DeterministicCommutativeEncryption] implementation using the JNI
 * [DeterministicCommutativeEncryptionUtility].
 */
class JniDeterministicCommutativeCryptor : Cryptor {
  /** Indicates something went wrong in C++. */
  class JniException(cause: Throwable) : RuntimeException(cause)

  private fun <T> wrapJniException(block: () -> T): T {
    return try {
      block()
    } catch (e: RuntimeException) {
      throw JniException(e)
    }
  }

  override fun encrypt(request: CryptorEncryptRequest): CryptorEncryptResponse {
    return wrapJniException {
      CryptorEncryptResponse.parseFrom(
        DeterministicCommutativeEncryptionUtility.deterministicCommutativeEncryptWrapper(
          request.toByteArray()
        )
      )
    }
  }

  override fun reEncrypt(request: CryptorReEncryptRequest): CryptorReEncryptResponse {
    return wrapJniException {
      CryptorReEncryptResponse.parseFrom(
        DeterministicCommutativeEncryptionUtility.deterministicCommutativeReEncryptWrapper(
          request.toByteArray()
        )
      )
    }
  }

  override fun decrypt(request: CryptorDecryptRequest): CryptorDecryptResponse {
    return wrapJniException {
      CryptorDecryptResponse.parseFrom(
        DeterministicCommutativeEncryptionUtility.deterministicCommutativeDecryptWrapper(
          request.toByteArray()
        )
      )
    }
  }

  companion object {
    init {
      loadLibrary(
        name = "deterministic_commutative_encryption_utility",
        directoryPath =
          Paths.get("panel_exchange_client/src/main/swig/wfanet/panelmatch/protocol/crypto")
      )
    }
  }
}
