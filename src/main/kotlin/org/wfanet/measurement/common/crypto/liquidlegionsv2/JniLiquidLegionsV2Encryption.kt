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

package org.wfanet.measurement.common.crypto.liquidlegionsv2

import java.nio.file.Paths
import org.wfanet.measurement.common.crypto.CompleteFilteringPhaseAtAggregatorRequest
import org.wfanet.measurement.common.crypto.CompleteFilteringPhaseAtAggregatorResponse
import org.wfanet.measurement.common.crypto.CompleteFilteringPhaseRequest
import org.wfanet.measurement.common.crypto.CompleteFilteringPhaseResponse
import org.wfanet.measurement.common.crypto.CompleteFrequencyEstimationPhaseAtAggregatorRequest
import org.wfanet.measurement.common.crypto.CompleteFrequencyEstimationPhaseAtAggregatorResponse
import org.wfanet.measurement.common.crypto.CompleteFrequencyEstimationPhaseRequest
import org.wfanet.measurement.common.crypto.CompleteFrequencyEstimationPhaseResponse
import org.wfanet.measurement.common.crypto.CompleteReachEstimationPhaseAtAggregatorRequest
import org.wfanet.measurement.common.crypto.CompleteReachEstimationPhaseAtAggregatorResponse
import org.wfanet.measurement.common.crypto.CompleteReachEstimationPhaseRequest
import org.wfanet.measurement.common.crypto.CompleteReachEstimationPhaseResponse
import org.wfanet.measurement.common.crypto.CompleteSetupPhaseRequest
import org.wfanet.measurement.common.crypto.CompleteSetupPhaseResponse
import org.wfanet.measurement.common.crypto.LiquidLegionsV2EncryptionUtility
import org.wfanet.measurement.common.loadLibrary

/**
 * A [LiquidLegionsV2Encryption] implementation using the JNI [LiquidLegionsV2EncryptionUtility].
 */
class JniLiquidLegionsV2Encryption : LiquidLegionsV2Encryption {

  override fun completeSetupPhase(request: CompleteSetupPhaseRequest): CompleteSetupPhaseResponse {
    return CompleteSetupPhaseResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeSetupPhase(request.toByteArray())
    )
  }

  override fun completeReachEstimationPhase(request: CompleteReachEstimationPhaseRequest):
    CompleteReachEstimationPhaseResponse {
      return CompleteReachEstimationPhaseResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeReachEstimationPhase(request.toByteArray())
      )
    }

  override fun completeReachEstimationPhaseAtAggregator(
    request: CompleteReachEstimationPhaseAtAggregatorRequest
  ): CompleteReachEstimationPhaseAtAggregatorResponse {
    return CompleteReachEstimationPhaseAtAggregatorResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeReachEstimationPhaseAtAggregator(
        request.toByteArray()
      )
    )
  }

  override fun completeFilteringPhase(request: CompleteFilteringPhaseRequest):
    CompleteFilteringPhaseResponse {
      return CompleteFilteringPhaseResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeFilteringPhase(request.toByteArray())
      )
    }

  override fun completeFilteringPhaseAtAggregator(
    request: CompleteFilteringPhaseAtAggregatorRequest
  ): CompleteFilteringPhaseAtAggregatorResponse {
    return CompleteFilteringPhaseAtAggregatorResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeFilteringPhaseAtAggregator(request.toByteArray())
    )
  }

  override fun completeFrequencyEstimationPhase(request: CompleteFrequencyEstimationPhaseRequest):
    CompleteFrequencyEstimationPhaseResponse {
      return CompleteFrequencyEstimationPhaseResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeFrequencyEstimationPhase(request.toByteArray())
      )
    }

  override fun completeFrequencyEstimationPhaseAtAggregator(
    request: CompleteFrequencyEstimationPhaseAtAggregatorRequest
  ): CompleteFrequencyEstimationPhaseAtAggregatorResponse {
    return CompleteFrequencyEstimationPhaseAtAggregatorResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeFrequencyEstimationPhaseAtAggregator(
        request.toByteArray()
      )
    )
  }

  companion object {
    init {
      loadLibrary(
        name = "liquid_legions_v2_encryption_utility",
        directoryPath = Paths.get(
          "wfa_measurement_system/src/main/swig/common/crypto/liquidlegionsv2"
        )
      )
    }
  }
}
