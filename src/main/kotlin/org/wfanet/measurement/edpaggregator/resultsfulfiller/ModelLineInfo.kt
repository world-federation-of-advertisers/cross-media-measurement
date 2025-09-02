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

import com.google.protobuf.Descriptors.Descriptor
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap

/**
 * Contains all information required to fulfill a requisition for a specific model line.
 *
 * @property populationSpec Specification of the population relevant to the model line.
 * @property eventDescriptor Descriptor for the event associated with the model line.
 * @property vidIndexMap Mapping of VIDs to their corresponding FrequencyVector indices for the
 *   model line.
 */
data class ModelLineInfo(
  val populationSpec: PopulationSpec,
  val eventDescriptor: Descriptor,
  val vidIndexMap: VidIndexMap,
)
