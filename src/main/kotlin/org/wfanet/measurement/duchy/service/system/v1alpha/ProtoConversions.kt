// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.service.system.v1alpha

import org.wfanet.measurement.duchy.ETags
import org.wfanet.measurement.internal.duchy.ComputationStage as InternalComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2
import org.wfanet.measurement.system.v1alpha.ComputationStage
import org.wfanet.measurement.system.v1alpha.HonestMajorityShareShuffleStage
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2Stage
import org.wfanet.measurement.system.v1alpha.ReachOnlyLiquidLegionsV2Stage
import org.wfanet.measurement.system.v1alpha.StageKey
import org.wfanet.measurement.system.v1alpha.computationStage
import org.wfanet.measurement.system.v1alpha.honestMajorityShareShuffleStage
import org.wfanet.measurement.system.v1alpha.liquidLegionsV2Stage
import org.wfanet.measurement.system.v1alpha.reachOnlyLiquidLegionsV2Stage

fun ComputationToken.toSystemStage(duchyId: String): ComputationStage {
  val source = this
  return computationStage {
    name = StageKey(source.globalComputationId, duchyId).toName()
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (source.computationStage.stageCase) {
      InternalComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 -> {
        liquidLegionsV2Stage =
          source.computationStage.liquidLegionsSketchAggregationV2.toSystemStage()
      }
      InternalComputationStage.StageCase.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 -> {
        reachOnlyLiquidLegionsStage =
          source.computationStage.reachOnlyLiquidLegionsSketchAggregationV2.toSystemStage()
      }
      InternalComputationStage.StageCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
        honestMajorityShareShuffleStage =
          source.computationStage.honestMajorityShareShuffle.toSystemStage()
      }
      InternalComputationStage.StageCase.TRUS_TEE ->
        error("TrusTEE protocol does not have system stage.")
      InternalComputationStage.StageCase.STAGE_NOT_SET -> error("Invalid stage case.")
    }
    etag = ETags.computeETag(source.version)
  }
}

private fun LiquidLegionsSketchAggregationV2.Stage.toSystemStage(): LiquidLegionsV2Stage {
  val internalStage = this
  return liquidLegionsV2Stage {
    stage =
      when (internalStage) {
        LiquidLegionsSketchAggregationV2.Stage.STAGE_UNSPECIFIED ->
          LiquidLegionsV2Stage.Stage.STAGE_UNSPECIFIED
        LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE ->
          LiquidLegionsV2Stage.Stage.INITIALIZATION_PHASE
        LiquidLegionsSketchAggregationV2.Stage.WAIT_REQUISITIONS_AND_KEY_SET ->
          LiquidLegionsV2Stage.Stage.WAIT_REQUISITIONS_AND_KEY_SET
        LiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE ->
          LiquidLegionsV2Stage.Stage.CONFIRMATION_PHASE
        LiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START ->
          LiquidLegionsV2Stage.Stage.WAIT_TO_START
        LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS ->
          LiquidLegionsV2Stage.Stage.WAIT_SETUP_PHASE_INPUTS
        LiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE -> LiquidLegionsV2Stage.Stage.SETUP_PHASE
        LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS ->
          LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS
        LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE ->
          LiquidLegionsV2Stage.Stage.EXECUTION_PHASE_ONE
        LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS ->
          LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS
        LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_TWO ->
          LiquidLegionsV2Stage.Stage.EXECUTION_PHASE_TWO
        LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS ->
          LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS
        LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_THREE ->
          LiquidLegionsV2Stage.Stage.EXECUTION_PHASE_THREE
        LiquidLegionsSketchAggregationV2.Stage.COMPLETE -> LiquidLegionsV2Stage.Stage.COMPLETE
        LiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED -> error("Invalid enum.")
      }
  }
}

private fun ReachOnlyLiquidLegionsSketchAggregationV2.Stage.toSystemStage():
  ReachOnlyLiquidLegionsV2Stage {
  val internalStage = this
  return reachOnlyLiquidLegionsV2Stage {
    stage =
      when (internalStage) {
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.STAGE_UNSPECIFIED ->
          ReachOnlyLiquidLegionsV2Stage.Stage.STAGE_UNSPECIFIED
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE ->
          ReachOnlyLiquidLegionsV2Stage.Stage.INITIALIZATION_PHASE
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_REQUISITIONS_AND_KEY_SET ->
          ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_REQUISITIONS_AND_KEY_SET
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE ->
          ReachOnlyLiquidLegionsV2Stage.Stage.CONFIRMATION_PHASE
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START ->
          ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_TO_START
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS ->
          ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_SETUP_PHASE_INPUTS
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE ->
          ReachOnlyLiquidLegionsV2Stage.Stage.SETUP_PHASE
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_INPUTS ->
          ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_INPUTS
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE ->
          ReachOnlyLiquidLegionsV2Stage.Stage.EXECUTION_PHASE
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.COMPLETE ->
          ReachOnlyLiquidLegionsV2Stage.Stage.COMPLETE
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED -> error("Invalid enum.")
      }
  }
}

private fun HonestMajorityShareShuffle.Stage.toSystemStage(): HonestMajorityShareShuffleStage {
  val internalStage = this
  return honestMajorityShareShuffleStage {
    stage =
      when (internalStage) {
        HonestMajorityShareShuffle.Stage.STAGE_UNSPECIFIED ->
          HonestMajorityShareShuffleStage.Stage.STAGE_UNSPECIFIED
        HonestMajorityShareShuffle.Stage.INITIALIZED ->
          HonestMajorityShareShuffleStage.Stage.INITIALIZED
        HonestMajorityShareShuffle.Stage.WAIT_TO_START ->
          HonestMajorityShareShuffleStage.Stage.WAIT_TO_START
        HonestMajorityShareShuffle.Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_ONE ->
          HonestMajorityShareShuffleStage.Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_ONE
        HonestMajorityShareShuffle.Stage.SETUP_PHASE ->
          HonestMajorityShareShuffleStage.Stage.SETUP_PHASE
        HonestMajorityShareShuffle.Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO ->
          HonestMajorityShareShuffleStage.Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO
        HonestMajorityShareShuffle.Stage.WAIT_ON_AGGREGATION_INPUT ->
          HonestMajorityShareShuffleStage.Stage.WAIT_ON_AGGREGATION_INPUT
        HonestMajorityShareShuffle.Stage.SHUFFLE_PHASE ->
          HonestMajorityShareShuffleStage.Stage.SHUFFLE_PHASE
        HonestMajorityShareShuffle.Stage.AGGREGATION_PHASE ->
          HonestMajorityShareShuffleStage.Stage.AGGREGATION_PHASE
        HonestMajorityShareShuffle.Stage.COMPLETE -> HonestMajorityShareShuffleStage.Stage.COMPLETE
        HonestMajorityShareShuffle.Stage.UNRECOGNIZED -> error("Invalid enum.")
      }
  }
}
