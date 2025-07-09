/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill

import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.TrusTee

enum class MillType {
  LIQUID_LEGIONS_V2,
  HONEST_MAJORITY_SHARE_SHUFFLE,
  TRUS_TEE,
}

val ComputationType.millType: MillType
  get() =
    when (this) {
      ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
      ComputationType.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 -> MillType.LIQUID_LEGIONS_V2
      ComputationType.HONEST_MAJORITY_SHARE_SHUFFLE -> MillType.HONEST_MAJORITY_SHARE_SHUFFLE
      ComputationType.TRUS_TEE -> MillType.TRUS_TEE
      ComputationType.UNSPECIFIED,
      ComputationType.UNRECOGNIZED -> error("Not a real computation type")
    }

val ComputationType.prioritizedStages: List<ComputationStage>
  get() =
    when (this) {
      ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 -> LIQUID_LEGIONS_V2_PRIORITIZED_STAGES
      ComputationType.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
        REACH_ONLY_LIQUID_LEGIONS_V2_PRIORITIZED_STAGES
      ComputationType.HONEST_MAJORITY_SHARE_SHUFFLE ->
        HONEST_MAJORITY_SHARE_SHUFFLE_PRIORITIZED_STAGES
      ComputationType.TRUS_TEE -> TRUS_TEE_PRIORITIZED_STAGES
      ComputationType.UNSPECIFIED,
      ComputationType.UNRECOGNIZED -> error("Not a real computation type")
    }

private val LIQUID_LEGIONS_V2_PRIORITIZED_STAGES =
  listOf(LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage())
private val REACH_ONLY_LIQUID_LEGIONS_V2_PRIORITIZED_STAGES =
  listOf(ReachOnlyLiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage())
private val HONEST_MAJORITY_SHARE_SHUFFLE_PRIORITIZED_STAGES =
  listOf(HonestMajorityShareShuffle.Stage.INITIALIZED.toProtocolStage())
private val TRUS_TEE_PRIORITIZED_STAGES = listOf(TrusTee.Stage.INITIALIZED.toProtocolStage())
