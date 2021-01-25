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

package org.wfanet.measurement.common.crypto.liquidlegionsv2

import org.wfanet.measurement.common.crypto.CompleteExecutionPhaseOneAtAggregatorRequest
import org.wfanet.measurement.common.crypto.CompleteExecutionPhaseOneAtAggregatorResponse
import org.wfanet.measurement.common.crypto.CompleteExecutionPhaseOneRequest
import org.wfanet.measurement.common.crypto.CompleteExecutionPhaseOneResponse
import org.wfanet.measurement.common.crypto.CompleteExecutionPhaseThreeAtAggregatorRequest
import org.wfanet.measurement.common.crypto.CompleteExecutionPhaseThreeAtAggregatorResponse
import org.wfanet.measurement.common.crypto.CompleteExecutionPhaseThreeRequest
import org.wfanet.measurement.common.crypto.CompleteExecutionPhaseThreeResponse
import org.wfanet.measurement.common.crypto.CompleteExecutionPhaseTwoAtAggregatorRequest
import org.wfanet.measurement.common.crypto.CompleteExecutionPhaseTwoAtAggregatorResponse
import org.wfanet.measurement.common.crypto.CompleteExecutionPhaseTwoRequest
import org.wfanet.measurement.common.crypto.CompleteExecutionPhaseTwoResponse
import org.wfanet.measurement.common.crypto.CompleteSetupPhaseRequest
import org.wfanet.measurement.common.crypto.CompleteSetupPhaseResponse

/**
 * Crypto operations for the Liquid Legions v2 protocol.
 * check src/main/cc/wfa/measurement/common/crypto/liquid_legions_v2_encryption_utility.h for more
 * descriptions.
 */
interface LiquidLegionsV2Encryption {

  fun completeSetupPhase(request: CompleteSetupPhaseRequest): CompleteSetupPhaseResponse

  fun completeExecutionPhaseOne(request: CompleteExecutionPhaseOneRequest):
    CompleteExecutionPhaseOneResponse

  fun completeExecutionPhaseOneAtAggregator(
    request: CompleteExecutionPhaseOneAtAggregatorRequest
  ):
    CompleteExecutionPhaseOneAtAggregatorResponse

  fun completeExecutionPhaseTwo(request: CompleteExecutionPhaseTwoRequest):
    CompleteExecutionPhaseTwoResponse

  fun completeExecutionPhaseTwoAtAggregator(request: CompleteExecutionPhaseTwoAtAggregatorRequest):
    CompleteExecutionPhaseTwoAtAggregatorResponse

  fun completeExecutionPhaseThree(request: CompleteExecutionPhaseThreeRequest):
    CompleteExecutionPhaseThreeResponse

  fun completeExecutionPhaseThreeAtAggregator(
    request: CompleteExecutionPhaseThreeAtAggregatorRequest
  ):
    CompleteExecutionPhaseThreeAtAggregatorResponse
}
