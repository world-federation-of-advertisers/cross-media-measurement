// Copyright 2020 The Cross-Media panelmatch Authors
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
import wfanet.panelmatch.protocol.crypto.CommutativeEncryptionUtility
import wfanet.panelmatch.protocol.protobuf.ApplyCommutativeDecryptionRequest
import wfanet.panelmatch.protocol.protobuf.ApplyCommutativeDecryptionResponse
import wfanet.panelmatch.protocol.protobuf.ApplyCommutativeEncryptionRequest
import wfanet.panelmatch.protocol.protobuf.ApplyCommutativeEncryptionResponse
import wfanet.panelmatch.protocol.protobuf.ReApplyCommutativeEncryptionRequest
import wfanet.panelmatch.protocol.protobuf.ReApplyCommutativeEncryptionResponse

/** A [CommutativeEncryption] implementation using the JNI [CommutativeEncryptionUtility]. */
class JniCommutativeEncryption : CommutativeEncryption {

  override fun applyCommutativeEncryption(
    request: ApplyCommutativeEncryptionRequest
  ): ApplyCommutativeEncryptionResponse {
    return ApplyCommutativeEncryptionResponse.parseFrom(
      CommutativeEncryptionUtility.applyCommutativeEncryptionWrapper(request.toByteArray())
    )
  }

  override fun reApplyCommutativeEncryption(
    request: ReApplyCommutativeEncryptionRequest
  ): ReApplyCommutativeEncryptionResponse {
    return ReApplyCommutativeEncryptionResponse.parseFrom(
      CommutativeEncryptionUtility.reApplyCommutativeEncryptionWrapper(request.toByteArray())
    )
  }

  override fun applyCommutativeDecryption(
    request: ApplyCommutativeDecryptionRequest
  ): ApplyCommutativeDecryptionResponse {
    return ApplyCommutativeDecryptionResponse.parseFrom(
      CommutativeEncryptionUtility.applyCommutativeDecryptionWrapper(request.toByteArray())
    )
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
