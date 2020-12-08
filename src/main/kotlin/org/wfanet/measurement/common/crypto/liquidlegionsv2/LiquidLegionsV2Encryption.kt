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

/**
 * Crypto operations for the Liquid Legions v2 protocol.
 * check src/main/cc/wfa/measurement/common/crypto/liquid_legions_v2_encryption_utility.h for more
 * descriptions.
 */
interface LiquidLegionsV2Encryption {

  fun completeSetupPhase(request: CompleteSetupPhaseRequest): CompleteSetupPhaseResponse

  fun completeReachEstimationPhase(request: CompleteReachEstimationPhaseRequest):
    CompleteReachEstimationPhaseResponse

  fun completeReachEstimationPhaseAtAggregator(
    request: CompleteReachEstimationPhaseAtAggregatorRequest
  ):
    CompleteReachEstimationPhaseAtAggregatorResponse

  fun completeFilteringPhase(request: CompleteFilteringPhaseRequest):
    CompleteFilteringPhaseResponse

  fun completeFilteringPhaseAtAggregator(request: CompleteFilteringPhaseAtAggregatorRequest):
    CompleteFilteringPhaseAtAggregatorResponse

  fun completeFrequencyEstimationPhase(request: CompleteFrequencyEstimationPhaseRequest):
    CompleteFrequencyEstimationPhaseResponse

  fun completeFrequencyEstimationPhaseAtAggregator(
    request: CompleteFrequencyEstimationPhaseAtAggregatorRequest
  ):
    CompleteFrequencyEstimationPhaseAtAggregatorResponse
}
