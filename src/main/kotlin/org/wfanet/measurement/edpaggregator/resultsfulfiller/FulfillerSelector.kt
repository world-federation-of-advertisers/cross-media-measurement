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

import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.MeasurementFulfiller

/**
 * Interface for selecting the appropriate MeasurementFulfiller based on protocol and requirements.
 */
interface FulfillerSelector {
  /**
   * Selects the appropriate MeasurementFulfiller for the given requisition and data.
   *
   * @param requisition The requisition to fulfill
   * @param measurementSpec The measurement specification
   * @param requisitionSpec The requisition specification
   * @param frequencyVector The frequency vector containing per-VID frequency counts
   * @param populationSpec The population specification
   * @return The selected MeasurementFulfiller
   */
  suspend fun selectFulfiller(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    requisitionSpec: RequisitionSpec,
    frequencyVector: StripedByteFrequencyVector,
    populationSpec: PopulationSpec,
  ): MeasurementFulfiller
}
