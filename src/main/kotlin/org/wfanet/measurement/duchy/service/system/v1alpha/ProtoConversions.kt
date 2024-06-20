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

import org.wfanet.measurement.internal.duchy.ComputationStage as InternalComputationStage
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle as InternalHmss
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2 as InternalLlv2
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2 as InternalRoLlv2
import org.wfanet.measurement.system.v1alpha.ComputationStage
import org.wfanet.measurement.system.v1alpha.ComputationStage.HonestMajorityShareShuffleStage
import org.wfanet.measurement.system.v1alpha.ComputationStage.LiquidLegionsV2Stage
import org.wfanet.measurement.system.v1alpha.ComputationStage.ReachOnlyLiquidLegionsV2Stage
import org.wfanet.measurement.system.v1alpha.computationStage

fun InternalComputationStage.toSystemComputationStage(): ComputationStage {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (stageCase) {
    InternalComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 -> {
      liquidLegionsSketchAggregationV2.toSystemComputationStage(etag)
    }
    InternalComputationStage.StageCase.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 -> {
      reachOnlyLiquidLegionsSketchAggregationV2.toSystemComputationStage(etag)
    }
    InternalComputationStage.StageCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
      honestMajorityShareShuffle.toSystemComputationStage(etag)
    }
    InternalComputationStage.StageCase.STAGE_NOT_SET -> error("Invalid internal ComputationStage.")
  }
}

private fun InternalLlv2.Stage.toSystemComputationStage(etag: String): ComputationStage {
  return when (this) {
    InternalLlv2.Stage.INITIALIZATION_PHASE ->
      LiquidLegionsV2Stage.Stage.INITIALIZATION_PHASE.toProtocolComputationStage(etag)
    InternalLlv2.Stage.WAIT_REQUISITIONS_AND_KEY_SET ->
      LiquidLegionsV2Stage.Stage.WAIT_REQUISITIONS_AND_KEY_SET.toProtocolComputationStage(etag)
    InternalLlv2.Stage.CONFIRMATION_PHASE ->
      LiquidLegionsV2Stage.Stage.CONFIRMATION_PHASE.toProtocolComputationStage(etag)
    InternalLlv2.Stage.WAIT_TO_START ->
      LiquidLegionsV2Stage.Stage.WAIT_TO_START.toProtocolComputationStage(etag)
    InternalLlv2.Stage.WAIT_SETUP_PHASE_INPUTS ->
      LiquidLegionsV2Stage.Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolComputationStage(etag)
    InternalLlv2.Stage.SETUP_PHASE ->
      LiquidLegionsV2Stage.Stage.SETUP_PHASE.toProtocolComputationStage(etag)
    InternalLlv2.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS ->
      LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolComputationStage(etag)
    InternalLlv2.Stage.EXECUTION_PHASE_ONE ->
      LiquidLegionsV2Stage.Stage.EXECUTION_PHASE_ONE.toProtocolComputationStage(etag)
    InternalLlv2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS ->
      LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolComputationStage(etag)
    InternalLlv2.Stage.EXECUTION_PHASE_TWO ->
      LiquidLegionsV2Stage.Stage.EXECUTION_PHASE_TWO.toProtocolComputationStage(etag)
    InternalLlv2.Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS ->
      LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS.toProtocolComputationStage(etag)
    InternalLlv2.Stage.EXECUTION_PHASE_THREE ->
      LiquidLegionsV2Stage.Stage.EXECUTION_PHASE_THREE.toProtocolComputationStage(etag)
    InternalLlv2.Stage.COMPLETE ->
      LiquidLegionsV2Stage.Stage.COMPLETE.toProtocolComputationStage(etag)
    InternalLlv2.Stage.UNRECOGNIZED,
    InternalLlv2.Stage.STAGE_UNSPECIFIED -> error("Invalid Llv2 stage.")
  }
}

private fun LiquidLegionsV2Stage.Stage.toProtocolComputationStage(etag: String) = computationStage {
  liquidLegionsV2Stage = this@toProtocolComputationStage
  this.etag = etag
}

private fun InternalRoLlv2.Stage.toSystemComputationStage(etag: String): ComputationStage {
  return when (this) {
    InternalRoLlv2.Stage.UNRECOGNIZED,
    InternalRoLlv2.Stage.STAGE_UNSPECIFIED -> error("Invalid RoLlv2 stage.")
    InternalRoLlv2.Stage.INITIALIZATION_PHASE ->
      ReachOnlyLiquidLegionsV2Stage.Stage.INITIALIZATION_PHASE.toProtocolComputationStage(etag)
    InternalRoLlv2.Stage.WAIT_REQUISITIONS_AND_KEY_SET ->
      ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_REQUISITIONS_AND_KEY_SET.toProtocolComputationStage(
        etag
      )
    InternalRoLlv2.Stage.CONFIRMATION_PHASE ->
      ReachOnlyLiquidLegionsV2Stage.Stage.CONFIRMATION_PHASE.toProtocolComputationStage(etag)
    InternalRoLlv2.Stage.WAIT_TO_START ->
      ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_TO_START.toProtocolComputationStage(etag)
    InternalRoLlv2.Stage.WAIT_SETUP_PHASE_INPUTS ->
      ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolComputationStage(etag)
    InternalRoLlv2.Stage.SETUP_PHASE ->
      ReachOnlyLiquidLegionsV2Stage.Stage.SETUP_PHASE.toProtocolComputationStage(etag)
    InternalRoLlv2.Stage.WAIT_EXECUTION_PHASE_INPUTS ->
      ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_INPUTS.toProtocolComputationStage(
        etag
      )
    InternalRoLlv2.Stage.EXECUTION_PHASE ->
      ReachOnlyLiquidLegionsV2Stage.Stage.EXECUTION_PHASE.toProtocolComputationStage(etag)
    InternalRoLlv2.Stage.COMPLETE ->
      ReachOnlyLiquidLegionsV2Stage.Stage.COMPLETE.toProtocolComputationStage(etag)
  }
}

private fun ReachOnlyLiquidLegionsV2Stage.Stage.toProtocolComputationStage(etag: String) =
  computationStage {
    reachOnlyLiquidLegionsV2Stage = this@toProtocolComputationStage
    this.etag = etag
  }

private fun InternalHmss.Stage.toSystemComputationStage(etag: String): ComputationStage {
  return when (this) {
    InternalHmss.Stage.UNRECOGNIZED,
    InternalHmss.Stage.STAGE_UNSPECIFIED -> error("Invalid Hmss stage.")
    InternalHmss.Stage.INITIALIZED ->
      HonestMajorityShareShuffleStage.Stage.INITIALIZED.toProtocolComputationStage(etag)
    InternalHmss.Stage.WAIT_TO_START ->
      HonestMajorityShareShuffleStage.Stage.WAIT_TO_START.toProtocolComputationStage(etag)
    InternalHmss.Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_ONE ->
      HonestMajorityShareShuffleStage.Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_ONE
        .toProtocolComputationStage(etag)
    InternalHmss.Stage.SETUP_PHASE ->
      HonestMajorityShareShuffleStage.Stage.SETUP_PHASE.toProtocolComputationStage(etag)
    InternalHmss.Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO ->
      HonestMajorityShareShuffleStage.Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO
        .toProtocolComputationStage(etag)
    InternalHmss.Stage.WAIT_ON_AGGREGATION_INPUT ->
      HonestMajorityShareShuffleStage.Stage.WAIT_ON_AGGREGATION_INPUT.toProtocolComputationStage(
        etag
      )
    InternalHmss.Stage.SHUFFLE_PHASE ->
      HonestMajorityShareShuffleStage.Stage.SHUFFLE_PHASE.toProtocolComputationStage(etag)
    InternalHmss.Stage.AGGREGATION_PHASE ->
      HonestMajorityShareShuffleStage.Stage.AGGREGATION_PHASE.toProtocolComputationStage(etag)
    InternalHmss.Stage.COMPLETE ->
      HonestMajorityShareShuffleStage.Stage.COMPLETE.toProtocolComputationStage(etag)
  }
}

private fun HonestMajorityShareShuffleStage.Stage.toProtocolComputationStage(etag: String) =
  computationStage {
    honestMajorityShareShuffleStage = this@toProtocolComputationStage
    this.etag = etag
  }
