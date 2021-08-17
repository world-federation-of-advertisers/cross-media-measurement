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

import java.nio.file.Paths
import org.wfanet.panelmatch.common.loadLibrary
import org.wfanet.panelmatch.common.wrapJniException
import org.wfanet.panelmatch.protocol.CryptorDecryptRequest
import org.wfanet.panelmatch.protocol.CryptorDecryptResponse
import org.wfanet.panelmatch.protocol.CryptorEncryptRequest
import org.wfanet.panelmatch.protocol.CryptorEncryptResponse
import org.wfanet.panelmatch.protocol.CryptorReEncryptRequest
import org.wfanet.panelmatch.protocol.CryptorReEncryptResponse
import org.wfanet.panelmatch.protocol.crypto.DeterministicCommutativeEncryptionWrapper

/**
 * A [DeterministicCommutativeEncryption] implementation using the JNI
 * [DeterministicCommutativeEncryptionWrapper].
 */
class JniDeterministicCommutativeCryptor : Cryptor {

  override fun encrypt(request: CryptorEncryptRequest): CryptorEncryptResponse {
    return wrapJniException {
      CryptorEncryptResponse.parseFrom(
        DeterministicCommutativeEncryptionWrapper.deterministicCommutativeEncryptWrapper(
          request.toByteArray()
        )
      )
    }
  }

  override fun reEncrypt(request: CryptorReEncryptRequest): CryptorReEncryptResponse {
    return wrapJniException {
      CryptorReEncryptResponse.parseFrom(
        DeterministicCommutativeEncryptionWrapper.deterministicCommutativeReEncryptWrapper(
          request.toByteArray()
        )
      )
    }
  }

  override fun decrypt(request: CryptorDecryptRequest): CryptorDecryptResponse {
    return wrapJniException {
      CryptorDecryptResponse.parseFrom(
        DeterministicCommutativeEncryptionWrapper.deterministicCommutativeDecryptWrapper(
          request.toByteArray()
        )
      )
    }
  }

  companion object {
    init {
      loadLibrary(
        name = "deterministic_commutative_encryption",
        directoryPath =
          Paths.get("panel_exchange_client/src/main/swig/wfanet/panelmatch/protocol/crypto")
      )
    }
  }
}
