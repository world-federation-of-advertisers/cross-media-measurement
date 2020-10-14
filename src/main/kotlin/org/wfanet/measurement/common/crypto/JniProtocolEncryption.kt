// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.common.crypto

import java.nio.file.Paths
import org.wfanet.measurement.common.loadLibrary

/**
 * A [ProtocolEncryption] implementation using the JNI
 * [ProtocolEncryptionUtility].
 */
class JniProtocolEncryption : ProtocolEncryption {

  override fun addNoiseToSketch(request: AddNoiseToSketchRequest): AddNoiseToSketchResponse {
    return AddNoiseToSketchResponse.parseFrom(
      ProtocolEncryptionUtility.addNoiseToSketch(request.toByteArray())
    )
  }

  override fun blindOneLayerRegisterIndex(
    request: BlindOneLayerRegisterIndexRequest
  ): BlindOneLayerRegisterIndexResponse {
    return BlindOneLayerRegisterIndexResponse.parseFrom(
      ProtocolEncryptionUtility.blindOneLayerRegisterIndex(request.toByteArray())
    )
  }

  override fun blindLastLayerIndexThenJoinRegisters(
    request: BlindLastLayerIndexThenJoinRegistersRequest
  ): BlindLastLayerIndexThenJoinRegistersResponse {
    return BlindLastLayerIndexThenJoinRegistersResponse.parseFrom(
      ProtocolEncryptionUtility.blindLastLayerIndexThenJoinRegisters(request.toByteArray())
    )
  }

  override fun decryptLastLayerFlagAndCount(
    request: DecryptLastLayerFlagAndCountRequest
  ): DecryptLastLayerFlagAndCountResponse {
    return DecryptLastLayerFlagAndCountResponse.parseFrom(
      ProtocolEncryptionUtility.decryptLastLayerFlagAndCount(request.toByteArray())
    )
  }

  override fun decryptOneLayerFlagAndCount(
    request: DecryptOneLayerFlagAndCountRequest
  ): DecryptOneLayerFlagAndCountResponse {
    return DecryptOneLayerFlagAndCountResponse.parseFrom(
      ProtocolEncryptionUtility.decryptOneLayerFlagAndCount(request.toByteArray())
    )
  }

  companion object {
    init {
      loadLibrary(
        name = "protocol_encryption_utility",
        directoryPath = Paths.get("wfa_measurement_system/src/main/swig/common/crypto")
      )
    }
  }
}
