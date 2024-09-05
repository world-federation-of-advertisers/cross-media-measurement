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
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage as Llv2Stage
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2.Stage as RoLlv2Stage
import org.wfanet.measurement.system.v1alpha.ComputationStage as SystemComputationStage
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2Stage
import org.wfanet.measurement.system.v1alpha.ReachOnlyLiquidLegionsV2Stage

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

fun SystemComputationStage.toComputationStage(): ComputationStage {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields are never null.
  return when (stageCase) {
    SystemComputationStage.StageCase.LIQUID_LEGIONS_V2_STAGE -> {
      when (this.liquidLegionsV2Stage.stage) {
        LiquidLegionsV2Stage.Stage.INITIALIZATION_PHASE ->
          Llv2Stage.INITIALIZATION_PHASE.toProtocolStage()
        LiquidLegionsV2Stage.Stage.WAIT_REQUISITIONS_AND_KEY_SET ->
          Llv2Stage.WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage()
        LiquidLegionsV2Stage.Stage.CONFIRMATION_PHASE ->
          Llv2Stage.CONFIRMATION_PHASE.toProtocolStage()
        LiquidLegionsV2Stage.Stage.WAIT_TO_START -> Llv2Stage.WAIT_TO_START.toProtocolStage()
        LiquidLegionsV2Stage.Stage.WAIT_SETUP_PHASE_INPUTS ->
          Llv2Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
        LiquidLegionsV2Stage.Stage.SETUP_PHASE -> Llv2Stage.SETUP_PHASE.toProtocolStage()
        LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS ->
          Llv2Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
        LiquidLegionsV2Stage.Stage.EXECUTION_PHASE_ONE ->
          Llv2Stage.EXECUTION_PHASE_ONE.toProtocolStage()
        LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS ->
          Llv2Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage()
        LiquidLegionsV2Stage.Stage.EXECUTION_PHASE_TWO ->
          Llv2Stage.EXECUTION_PHASE_TWO.toProtocolStage()
        LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS ->
          Llv2Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS.toProtocolStage()
        LiquidLegionsV2Stage.Stage.EXECUTION_PHASE_THREE ->
          Llv2Stage.EXECUTION_PHASE_THREE.toProtocolStage()
        LiquidLegionsV2Stage.Stage.COMPLETE -> Llv2Stage.COMPLETE.toProtocolStage()
        LiquidLegionsV2Stage.Stage.STAGE_UNSPECIFIED,
        LiquidLegionsV2Stage.Stage.UNRECOGNIZED -> error("Invalid LLv2 Stage")
      }
    }
    SystemComputationStage.StageCase.REACH_ONLY_LIQUID_LEGIONS_STAGE -> {
      when (this.reachOnlyLiquidLegionsStage.stage) {
        ReachOnlyLiquidLegionsV2Stage.Stage.INITIALIZATION_PHASE ->
          RoLlv2Stage.INITIALIZATION_PHASE.toProtocolStage()
        ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_REQUISITIONS_AND_KEY_SET ->
          RoLlv2Stage.WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage()
        ReachOnlyLiquidLegionsV2Stage.Stage.CONFIRMATION_PHASE ->
          RoLlv2Stage.CONFIRMATION_PHASE.toProtocolStage()
        ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_TO_START ->
          RoLlv2Stage.WAIT_TO_START.toProtocolStage()
        ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_SETUP_PHASE_INPUTS ->
          RoLlv2Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
        ReachOnlyLiquidLegionsV2Stage.Stage.SETUP_PHASE -> RoLlv2Stage.SETUP_PHASE.toProtocolStage()
        ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_INPUTS ->
          RoLlv2Stage.WAIT_EXECUTION_PHASE_INPUTS.toProtocolStage()
        ReachOnlyLiquidLegionsV2Stage.Stage.EXECUTION_PHASE ->
          RoLlv2Stage.EXECUTION_PHASE.toProtocolStage()
        ReachOnlyLiquidLegionsV2Stage.Stage.COMPLETE -> Llv2Stage.COMPLETE.toProtocolStage()
        ReachOnlyLiquidLegionsV2Stage.Stage.STAGE_UNSPECIFIED,
        ReachOnlyLiquidLegionsV2Stage.Stage.UNRECOGNIZED -> error("Invalid RoLLv2 Stage")
      }
    }
    SystemComputationStage.StageCase.HONEST_MAJORITY_SHARE_SHUFFLE_STAGE -> {
      error("Invalid SystemComputationStage")
    }
    SystemComputationStage.StageCase.STAGE_NOT_SET -> error("Invalid SystemComputationStage")
  }
}
