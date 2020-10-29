// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.duchy.db.computation

import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.COMPLETED
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.SKETCH_AGGREGATION_STAGE_UNKNOWN
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_ADD_NOISE
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_CONFIRM_REQUISITIONS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.UNRECOGNIZED
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_CONCATENATED
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_FLAG_COUNTS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_SKETCHES
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_TO_START
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.ComputationDetails.RoleInComputation
import org.wfanet.measurement.internal.duchy.ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub

/**
 * Calls AdvanceComputationStage to move to a new [LiquidLegionsSketchAggregationStage] in a
 * consistent way.
 *
 * The assumption is this will only be called by a job that is executing the stage of a
 * computation, which will have knowledge of all the data needed as input to the next stage.
 * Most of the time [inputsToNextStage] is the list of outputs of the currently running stage.
 */
suspend fun ComputationsCoroutineStub.advanceLiquidLegionsComputationStage(
  computationToken: ComputationToken,
  inputsToNextStage: List<String>,
  stage: LiquidLegionsSketchAggregationStage,
  liquidLegionsStageDetails: LiquidLegionsSketchAggregationProtocol.EnumStages.Details
): ComputationToken {
  require(computationToken.computationStage.stageCase == LIQUID_LEGIONS_SKETCH_AGGREGATION) {
    "Must be a token for a LIQUID_LEGIONS_SKETCH_AGGREGATION computation was $computationToken."
  }
  requireValidRoleForStage(stage, computationToken.role)
  requireNotEmpty(inputsToNextStage)
  val request: AdvanceComputationStageRequest =
    AdvanceComputationStageRequest.newBuilder().apply {
      token = computationToken
      nextComputationStage = stage.toProtocolStage()
      addAllInputBlobs(inputsToNextStage)
      stageDetails = liquidLegionsStageDetails.detailsFor(stage)
      afterTransition = afterTransitionForStage(stage)
      outputBlobs = when (stage) {
        WAIT_TO_START ->
          // There is no output in this stage, the input is forwarded to the next stage as input.
          0
        WAIT_CONCATENATED,
        WAIT_FLAG_COUNTS,
        TO_ADD_NOISE,
        TO_APPEND_SKETCHES_AND_ADD_NOISE,
        TO_BLIND_POSITIONS,
        TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
        TO_DECRYPT_FLAG_COUNTS,
        TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS ->
          // The output is the intermediate computation result either received from another duchy
          // or computed locally.
          1
        WAIT_SKETCHES ->
          // The output contains otherDuchiesInComputation sketches from the other duchies.
          liquidLegionsStageDetails.otherDuchies.size
        // Mill have nothing to do for this stage.
        COMPLETED -> error("Computation should be ended with call to endComputation(...)")
        // Stages that we can't transition to ever.
        UNRECOGNIZED, SKETCH_AGGREGATION_STAGE_UNKNOWN, TO_CONFIRM_REQUISITIONS ->
          error("Cannot make transition function to stage $stage")
      }
    }.build()
  return this.advanceComputationStage(request).token
}

private fun afterTransitionForStage(
  stage: LiquidLegionsSketchAggregationStage
): AdvanceComputationStageRequest.AfterTransition =
  when (stage) {
    // Stages of computation mapping some number of inputs to single output.
    TO_ADD_NOISE,
    TO_APPEND_SKETCHES_AND_ADD_NOISE,
    TO_BLIND_POSITIONS,
    TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
    TO_DECRYPT_FLAG_COUNTS,
    TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS ->
      AdvanceComputationStageRequest.AfterTransition.ADD_UNCLAIMED_TO_QUEUE
    WAIT_TO_START,
    WAIT_SKETCHES,
    WAIT_CONCATENATED,
    WAIT_FLAG_COUNTS ->
      AdvanceComputationStageRequest.AfterTransition.DO_NOT_ADD_TO_QUEUE
    COMPLETED -> error("Computation should be ended with call to endComputation(...)")
    // Stages that we can't transition to ever.
    UNRECOGNIZED, SKETCH_AGGREGATION_STAGE_UNKNOWN, TO_CONFIRM_REQUISITIONS ->
      error("Cannot make transition function to stage $stage")
  }

private fun requireValidRoleForStage(
  stage: LiquidLegionsSketchAggregationStage,
  role: RoleInComputation
) {
  when (stage) {
    WAIT_SKETCHES,
    TO_APPEND_SKETCHES_AND_ADD_NOISE,
    TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
    TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS ->
      require(role == RoleInComputation.PRIMARY) {
        "$stage may only be executed by the primary MPC worker."
      }
    WAIT_TO_START,
    TO_ADD_NOISE,
    TO_BLIND_POSITIONS,
    TO_DECRYPT_FLAG_COUNTS ->
      require(role == RoleInComputation.SECONDARY) {
        "$stage may only be executed by a non-primary MPC worker."
      }
    else -> { /* Stage can be executed at either primary or non-primary */
    }
  }
}

private fun requireNotEmpty(paths: List<String>): List<String> {
  require(paths.isNotEmpty()) { "Passed paths to input blobs is empty" }
  return paths
}
