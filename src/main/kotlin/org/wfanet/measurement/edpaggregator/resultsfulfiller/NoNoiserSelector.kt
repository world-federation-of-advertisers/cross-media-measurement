/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

/** Only selects no noise. */
class NoNoiserSelector : NoiserSelector {
  /**
   * Selects the most preferred [DirectNoiseMechanism] for reach and frequency measurements from the
   * overlap of a list of options of [NoiseMechanism].
   */
  override fun selectNoiseMechanism(options: List<NoiseMechanism>): DirectNoiseMechanism {
    return if (options.contains(NoiseMechanism.NONE)) {
      DirectNoiseMechanism.NONE
    } else {
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.SPEC_INVALID,
        "No valid noise mechanism option for reach or frequency measurements.",
      )
    }
  }
}
