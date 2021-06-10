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

package org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.crypto

import java.nio.file.Paths
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysRequest
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseOneAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseOneAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseOneRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseOneResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseThreeAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseThreeAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseThreeRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseThreeResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseTwoAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseTwoAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseTwoRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseTwoResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteInitializationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteInitializationPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteSetupPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteSetupPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.liquidlegionsv2.LiquidLegionsV2EncryptionUtility

/**
 * A [LiquidLegionsV2Encryption] implementation using the JNI [LiquidLegionsV2EncryptionUtility].
 */
class JniLiquidLegionsV2Encryption : LiquidLegionsV2Encryption {

  override fun completeInitializationPhase(
    request: CompleteInitializationPhaseRequest
  ): CompleteInitializationPhaseResponse {
    return CompleteInitializationPhaseResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeInitializationPhase(request.toByteArray())
    )
  }

  override fun completeSetupPhase(request: CompleteSetupPhaseRequest): CompleteSetupPhaseResponse {
    return CompleteSetupPhaseResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeSetupPhase(request.toByteArray())
    )
  }

  override fun completeExecutionPhaseOne(
    request: CompleteExecutionPhaseOneRequest
  ): CompleteExecutionPhaseOneResponse {
    return CompleteExecutionPhaseOneResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeExecutionPhaseOne(request.toByteArray())
    )
  }

  override fun completeExecutionPhaseOneAtAggregator(
    request: CompleteExecutionPhaseOneAtAggregatorRequest
  ): CompleteExecutionPhaseOneAtAggregatorResponse {
    return CompleteExecutionPhaseOneAtAggregatorResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeExecutionPhaseOneAtAggregator(request.toByteArray())
    )
  }

  override fun completeExecutionPhaseTwo(
    request: CompleteExecutionPhaseTwoRequest
  ): CompleteExecutionPhaseTwoResponse {
    return CompleteExecutionPhaseTwoResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeExecutionPhaseTwo(request.toByteArray())
    )
  }

  override fun completeExecutionPhaseTwoAtAggregator(
    request: CompleteExecutionPhaseTwoAtAggregatorRequest
  ): CompleteExecutionPhaseTwoAtAggregatorResponse {
    return CompleteExecutionPhaseTwoAtAggregatorResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeExecutionPhaseTwoAtAggregator(request.toByteArray())
    )
  }

  override fun completeExecutionPhaseThree(
    request: CompleteExecutionPhaseThreeRequest
  ): CompleteExecutionPhaseThreeResponse {
    return CompleteExecutionPhaseThreeResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeExecutionPhaseThree(request.toByteArray())
    )
  }

  override fun completeExecutionPhaseThreeAtAggregator(
    request: CompleteExecutionPhaseThreeAtAggregatorRequest
  ): CompleteExecutionPhaseThreeAtAggregatorResponse {
    return CompleteExecutionPhaseThreeAtAggregatorResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeExecutionPhaseThreeAtAggregator(
        request.toByteArray()
      )
    )
  }

  override fun combineElGamalPublicKeys(
    request: CombineElGamalPublicKeysRequest
  ): CombineElGamalPublicKeysResponse {
    return CombineElGamalPublicKeysResponse.parseFrom(
      SketchEncrypterAdapter.CombineElGamalPublicKeys(request.toByteArray())
    )
  }

  companion object {
    init {
      loadLibrary(
        name = "liquid_legions_v2_encryption_utility",
        directoryPath = Paths.get("wfa_measurement_system/src/main/swig/protocol/liquidlegionsv2")
      )
      loadLibrary(
        name = "sketch_encrypter_adapter",
        directoryPath = Paths.get("any_sketch_java/src/main/java/org/wfanet/anysketch/crypto")
      )
    }
  }
}
