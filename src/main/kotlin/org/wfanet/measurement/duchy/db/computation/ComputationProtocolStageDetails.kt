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

package org.wfanet.measurement.duchy.db.computation

import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType

/** Deals with stage specific details for a computation protocol. */
object ComputationProtocolStageDetails :
  ComputationProtocolStageDetailsHelper<
    ComputationType, ComputationStage, ComputationStageDetails, ComputationDetails
  > {

  override fun validateRoleForStage(
    stage: ComputationStage,
    computationDetails: ComputationDetails
  ): Boolean {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (stage.stageCase) {
      ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
        LiquidLegionsSketchAggregationV2Protocol.ComputationStages.Details.validateRoleForStage(
          stage,
          computationDetails
        )
      ComputationStage.StageCase.STAGE_NOT_SET -> error("Stage not set")
    }
  }

  override fun afterTransitionForStage(stage: ComputationStage): AfterTransition {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (stage.stageCase) {
      ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
        LiquidLegionsSketchAggregationV2Protocol.ComputationStages.Details.afterTransitionForStage(
          stage
        )
      ComputationStage.StageCase.STAGE_NOT_SET -> error("Stage not set")
    }
  }

  override fun outputBlobNumbersForStage(
    stage: ComputationStage,
    computationDetails: ComputationDetails
  ): Int {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (stage.stageCase) {
      ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
        LiquidLegionsSketchAggregationV2Protocol.ComputationStages.Details
          .outputBlobNumbersForStage(stage, computationDetails)
      ComputationStage.StageCase.STAGE_NOT_SET -> error("Stage not set")
    }
  }

  override fun detailsFor(
    stage: ComputationStage,
    computationDetails: ComputationDetails
  ): ComputationStageDetails {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (stage.stageCase) {
      ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
        LiquidLegionsSketchAggregationV2Protocol.ComputationStages.Details.detailsFor(
          stage,
          computationDetails
        )
      ComputationStage.StageCase.STAGE_NOT_SET -> error("Stage not set")
    }
  }

  override fun parseDetails(protocol: ComputationType, bytes: ByteArray): ComputationStageDetails {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (protocol) {
      ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
        LiquidLegionsSketchAggregationV2Protocol.ComputationStages.Details.parseDetails(bytes)
      ComputationType.UNSPECIFIED,
      ComputationType.UNRECOGNIZED -> error("invalid protocol")
    }
  }

  override fun setEndingState(
    details: ComputationDetails,
    reason: EndComputationReason
  ): ComputationDetails {
    return details.toBuilder().setEndingState(reason.toCompletedReason()).build()
  }

  override fun parseComputationDetails(bytes: ByteArray): ComputationDetails =
    ComputationDetails.parseFrom(bytes)
}

fun EndComputationReason.toCompletedReason(): ComputationDetails.CompletedReason {
  return when (this) {
    EndComputationReason.SUCCEEDED -> ComputationDetails.CompletedReason.SUCCEEDED
    EndComputationReason.FAILED -> ComputationDetails.CompletedReason.FAILED
    EndComputationReason.CANCELED -> ComputationDetails.CompletedReason.CANCELED
  }
}
