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
import wfanet.panelmatch.protocol.crypto.CommutativeEncryptionUtility
import wfanet.panelmatch.protocol.protobuf.ApplyCommutativeDecryptionRequest
import wfanet.panelmatch.protocol.protobuf.ApplyCommutativeDecryptionResponse
import wfanet.panelmatch.protocol.protobuf.ApplyCommutativeEncryptionRequest
import wfanet.panelmatch.protocol.protobuf.ApplyCommutativeEncryptionResponse
import wfanet.panelmatch.protocol.protobuf.ReApplyCommutativeEncryptionRequest
import wfanet.panelmatch.protocol.protobuf.ReApplyCommutativeEncryptionResponse

/** A [CommutativeEncryption] implementation using the JNI [CommutativeEncryptionUtility]. */
class JniCommutativeEncryption : CommutativeEncryption {
  /** Indicates something went wrong in C++. */
  class JniException(cause: Throwable) : RuntimeException(cause)

  private fun <T> wrapJniException(block: () -> T): T {
    return try {
      block()
    } catch (e: RuntimeException) {
      throw JniException(e)
    }
  }

  override fun encrypt(
    request: ApplyCommutativeEncryptionRequest
  ): ApplyCommutativeEncryptionResponse {
    return wrapJniException {
      ApplyCommutativeEncryptionResponse.parseFrom(
        CommutativeEncryptionUtility.applyCommutativeEncryptionWrapper(request.toByteArray())
      )
    }
  }

  override fun reEncrypt(
    request: ReApplyCommutativeEncryptionRequest
  ): ReApplyCommutativeEncryptionResponse {
    return wrapJniException {
      ReApplyCommutativeEncryptionResponse.parseFrom(
        CommutativeEncryptionUtility.reApplyCommutativeEncryptionWrapper(request.toByteArray())
      )
    }
  }

  override fun decrypt(
    request: ApplyCommutativeDecryptionRequest
  ): ApplyCommutativeDecryptionResponse {
    return wrapJniException {
      ApplyCommutativeDecryptionResponse.parseFrom(
        CommutativeEncryptionUtility.applyCommutativeDecryptionWrapper(request.toByteArray())
      )
    }
  }

  companion object {
    init {
      loadLibrary(
        name = "commutative_encryption_utility",
        directoryPath =
          Paths.get("panel_exchange_client/src/main/swig/wfanet/panelmatch/protocol/crypto")
      )
    }
  }
}
