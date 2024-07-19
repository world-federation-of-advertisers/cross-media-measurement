// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill.liquidlegionsv2.crypto

import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysRequest
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyInitializationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyInitializationPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlySetupPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlySetupPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.reachonlyliquidlegionsv2.ReachOnlyLiquidLegionsV2EncryptionUtility

/**
 * A [ReachOnlyLiquidLegionsV2Encryption] implementation using the JNI
 * [ReachOnlyLiquidLegionsV2EncryptionUtility].
 */
class JniReachOnlyLiquidLegionsV2Encryption : ReachOnlyLiquidLegionsV2Encryption {

  override fun completeReachOnlyInitializationPhase(
    request: CompleteReachOnlyInitializationPhaseRequest
  ): CompleteReachOnlyInitializationPhaseResponse {
    return CompleteReachOnlyInitializationPhaseResponse.parseFrom(
      ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlyInitializationPhase(
        request.toByteArray()
      )
    )
  }

  override fun completeReachOnlySetupPhase(
    request: CompleteReachOnlySetupPhaseRequest
  ): CompleteReachOnlySetupPhaseResponse {
    return CompleteReachOnlySetupPhaseResponse.parseFrom(
      ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlySetupPhase(request.toByteArray())
    )
  }

  override fun completeReachOnlySetupPhaseAtAggregator(
    request: CompleteReachOnlySetupPhaseRequest
  ): CompleteReachOnlySetupPhaseResponse {
    return CompleteReachOnlySetupPhaseResponse.parseFrom(
      ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlySetupPhaseAtAggregator(
        request.toByteArray()
      )
    )
  }

  override fun completeReachOnlyExecutionPhase(
    request: CompleteReachOnlyExecutionPhaseRequest
  ): CompleteReachOnlyExecutionPhaseResponse {
    return CompleteReachOnlyExecutionPhaseResponse.parseFrom(
      ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlyExecutionPhase(
        request.toByteArray()
      )
    )
  }

  override fun completeReachOnlyExecutionPhaseAtAggregator(
    request: CompleteReachOnlyExecutionPhaseAtAggregatorRequest
  ): CompleteReachOnlyExecutionPhaseAtAggregatorResponse {
    return CompleteReachOnlyExecutionPhaseAtAggregatorResponse.parseFrom(
      ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlyExecutionPhaseAtAggregator(
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
      System.loadLibrary("reach_only_liquid_legions_v2_encryption_utility")
      System.loadLibrary("sketch_encrypter_adapter")
    }
  }
}
