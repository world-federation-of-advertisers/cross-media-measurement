// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv1.crypto

import java.nio.file.Paths
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.protocol.AddNoiseToSketchRequest
import org.wfanet.measurement.protocol.AddNoiseToSketchResponse
import org.wfanet.measurement.protocol.BlindLastLayerIndexThenJoinRegistersRequest
import org.wfanet.measurement.protocol.BlindLastLayerIndexThenJoinRegistersResponse
import org.wfanet.measurement.protocol.BlindOneLayerRegisterIndexRequest
import org.wfanet.measurement.protocol.BlindOneLayerRegisterIndexResponse
import org.wfanet.measurement.protocol.DecryptLastLayerFlagAndCountRequest
import org.wfanet.measurement.protocol.DecryptLastLayerFlagAndCountResponse
import org.wfanet.measurement.protocol.DecryptOneLayerFlagAndCountRequest
import org.wfanet.measurement.protocol.DecryptOneLayerFlagAndCountResponse
import org.wfanet.measurement.protocol.liquidlegionsv1.LiquidLegionsV1EncryptionUtility

/**
 * A [LiquidLegionsV1Encryption] implementation using the JNI [LiquidLegionsV1EncryptionUtility].
 */
class JniLiquidLegionsV1Encryption : LiquidLegionsV1Encryption {

  override fun addNoiseToSketch(request: AddNoiseToSketchRequest): AddNoiseToSketchResponse {
    return AddNoiseToSketchResponse.parseFrom(
      LiquidLegionsV1EncryptionUtility.addNoiseToSketch(request.toByteArray())
    )
  }

  override fun blindOneLayerRegisterIndex(
    request: BlindOneLayerRegisterIndexRequest
  ): BlindOneLayerRegisterIndexResponse {
    return BlindOneLayerRegisterIndexResponse.parseFrom(
      LiquidLegionsV1EncryptionUtility.blindOneLayerRegisterIndex(request.toByteArray())
    )
  }

  override fun blindLastLayerIndexThenJoinRegisters(
    request: BlindLastLayerIndexThenJoinRegistersRequest
  ): BlindLastLayerIndexThenJoinRegistersResponse {
    return BlindLastLayerIndexThenJoinRegistersResponse.parseFrom(
      LiquidLegionsV1EncryptionUtility.blindLastLayerIndexThenJoinRegisters(request.toByteArray())
    )
  }

  override fun decryptLastLayerFlagAndCount(
    request: DecryptLastLayerFlagAndCountRequest
  ): DecryptLastLayerFlagAndCountResponse {
    return DecryptLastLayerFlagAndCountResponse.parseFrom(
      LiquidLegionsV1EncryptionUtility.decryptLastLayerFlagAndCount(request.toByteArray())
    )
  }

  override fun decryptOneLayerFlagAndCount(
    request: DecryptOneLayerFlagAndCountRequest
  ): DecryptOneLayerFlagAndCountResponse {
    return DecryptOneLayerFlagAndCountResponse.parseFrom(
      LiquidLegionsV1EncryptionUtility.decryptOneLayerFlagAndCount(request.toByteArray())
    )
  }

  companion object {
    init {
      loadLibrary(
        name = "liquid_legions_v1_encryption_utility",
        directoryPath = Paths.get(
          "wfa_measurement_system/src/main/swig/protocol/liquidlegionsv1"
        )
      )
    }
  }
}
