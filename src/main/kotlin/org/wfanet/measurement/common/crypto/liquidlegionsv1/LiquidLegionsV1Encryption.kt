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

package org.wfanet.measurement.common.crypto.liquidlegionsv1

import org.wfanet.measurement.common.crypto.AddNoiseToSketchRequest
import org.wfanet.measurement.common.crypto.AddNoiseToSketchResponse
import org.wfanet.measurement.common.crypto.BlindLastLayerIndexThenJoinRegistersRequest
import org.wfanet.measurement.common.crypto.BlindLastLayerIndexThenJoinRegistersResponse
import org.wfanet.measurement.common.crypto.BlindOneLayerRegisterIndexRequest
import org.wfanet.measurement.common.crypto.BlindOneLayerRegisterIndexResponse
import org.wfanet.measurement.common.crypto.DecryptLastLayerFlagAndCountRequest
import org.wfanet.measurement.common.crypto.DecryptLastLayerFlagAndCountResponse
import org.wfanet.measurement.common.crypto.DecryptOneLayerFlagAndCountRequest
import org.wfanet.measurement.common.crypto.DecryptOneLayerFlagAndCountResponse

/** Crypto operations for the Liquid Legions V1 protocol. */
interface LiquidLegionsV1Encryption {
  /**
   * Add noise registers to the input sketch.
   */
  fun addNoiseToSketch(request: AddNoiseToSketchRequest): AddNoiseToSketchResponse

  /**
   * Blind (one layer) all register indexes of a sketch.
   */
  fun blindOneLayerRegisterIndex(
    request: BlindOneLayerRegisterIndexRequest
  ): BlindOneLayerRegisterIndexResponse

  /**
   * Blind (last layer) the register indexes, and then join the registers by the deterministically
   * encrypted register indexes, and then merge the counts using the same-key-aggregating algorithm.
   */
  fun blindLastLayerIndexThenJoinRegisters(
    request: BlindLastLayerIndexThenJoinRegistersRequest
  ): BlindLastLayerIndexThenJoinRegistersResponse

  /**
   * Decrypt (one layer) the count and flag of all registers.
   */
  fun decryptOneLayerFlagAndCount(
    request: DecryptOneLayerFlagAndCountRequest
  ): DecryptOneLayerFlagAndCountResponse

  /**
   * Decrypt (last layer) the count and flag of all registers.
   */
  fun decryptLastLayerFlagAndCount(
    request: DecryptLastLayerFlagAndCountRequest
  ): DecryptLastLayerFlagAndCountResponse
}
