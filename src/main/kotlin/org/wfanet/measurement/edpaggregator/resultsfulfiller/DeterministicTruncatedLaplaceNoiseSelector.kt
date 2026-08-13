/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.dataprovider.RequisitionRefusalException
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

/**
 * Selects deterministic truncated-Laplace noise, refusing the requisition when the Kingdom does not
 * offer it.
 *
 * The EDP opts in through its own configuration, so there is no Direct capability gate: a Kingdom
 * that does not list the mechanism has not agreed to it for this measurement.
 */
class DeterministicTruncatedLaplaceNoiseSelector : NoiserSelector {
  override fun selectNoiseMechanism(options: List<NoiseMechanism>): DirectNoiseMechanism {
    return if (options.contains(NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)) {
      DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE
    } else {
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.SPEC_INVALID,
        "No valid noise mechanism option for reach or frequency measurements.",
      )
    }
  }
}
