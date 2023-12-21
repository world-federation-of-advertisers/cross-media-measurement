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

package org.wfanet.measurement.duchy

import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.computationStage
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2

val ComputationStage.name: String
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  get() =
    when (stageCase) {
      ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
        liquidLegionsSketchAggregationV2.name
      ComputationStage.StageCase.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
        reachOnlyLiquidLegionsSketchAggregationV2.name
      ComputationStage.StageCase.HONEST_MAJORITY_SHARE_SHUFFLE -> honestMajorityShareShuffle.name
      ComputationStage.StageCase.STAGE_NOT_SET -> error("Stage not set")
    }

val ComputationStage.number: Int
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  get() =
    when (stageCase) {
      ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
        liquidLegionsSketchAggregationV2.number
      ComputationStage.StageCase.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
        reachOnlyLiquidLegionsSketchAggregationV2.number
      ComputationStage.StageCase.HONEST_MAJORITY_SHARE_SHUFFLE -> honestMajorityShareShuffle.number
      ComputationStage.StageCase.STAGE_NOT_SET -> error("Stage not set")
    }

fun LiquidLegionsSketchAggregationV2.Stage.toProtocolStage(): ComputationStage = computationStage {
  liquidLegionsSketchAggregationV2 = this@toProtocolStage
}

fun ReachOnlyLiquidLegionsSketchAggregationV2.Stage.toProtocolStage(): ComputationStage =
  computationStage {
    reachOnlyLiquidLegionsSketchAggregationV2 = this@toProtocolStage
  }

fun HonestMajorityShareShuffle.Stage.toProtocolStage(): ComputationStage = computationStage {
  honestMajorityShareShuffle = this@toProtocolStage
}
