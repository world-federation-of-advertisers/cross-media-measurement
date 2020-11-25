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
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType.UNRECOGNIZED
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType.UNSPECIFIED

/** Provides methods for working with an enum representation of stages for MPC protocols */
object ComputationProtocolStages :
  ComputationProtocolStagesEnumHelper<ComputationType, ComputationStage> {

  override fun stageToProtocol(stage: ComputationStage): ComputationType {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (stage.stageCase) {
      ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1 ->
        LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
      ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
        LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
      ComputationStage.StageCase.STAGE_NOT_SET -> error("Stage not set")
    }
  }

  override fun computationStageEnumToLongValues(value: ComputationStage):
    ComputationStageLongValues {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      return when (value.stageCase) {
        ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1 ->
          ComputationStageLongValues(
            ComputationTypes.protocolEnumToLong(LIQUID_LEGIONS_SKETCH_AGGREGATION_V1),
            LiquidLegionsSketchAggregationV1Protocol.EnumStages.enumToLong(
              value.liquidLegionsSketchAggregationV1
            )
          )
        ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
          ComputationStageLongValues(
            ComputationTypes.protocolEnumToLong(LIQUID_LEGIONS_SKETCH_AGGREGATION_V2),
            LiquidLegionsSketchAggregationV2Protocol.EnumStages.enumToLong(
              value.liquidLegionsSketchAggregationV2
            )
          )
        ComputationStage.StageCase.STAGE_NOT_SET -> error("Stage not set")
      }
    }

  override fun longValuesToComputationStageEnum(value: ComputationStageLongValues):
    ComputationStage {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      return when (ComputationTypes.longToProtocolEnum(value.protocol)) {
        LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
        -> LiquidLegionsSketchAggregationV1Protocol.EnumStages.longToEnum(value.stage)
          .toProtocolStage()
        LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
        -> LiquidLegionsSketchAggregationV2Protocol.EnumStages.longToEnum(value.stage)
          .toProtocolStage()
        UNSPECIFIED, UNRECOGNIZED -> error("protocol not set")
      }
    }

  override fun getValidInitialStage(protocol: ComputationType): Set<ComputationStage> {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (protocol) {
      LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
      -> LiquidLegionsSketchAggregationV1Protocol.ComputationStages.validInitialStages
      LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
      -> LiquidLegionsSketchAggregationV2Protocol.ComputationStages.validInitialStages
      UNSPECIFIED, UNRECOGNIZED -> error("protocol not set")
    }
  }

  override fun getValidTerminalStages(protocol: ComputationType): Set<ComputationStage> {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (protocol) {
      LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
      -> LiquidLegionsSketchAggregationV1Protocol.ComputationStages.validTerminalStages
      LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
      -> LiquidLegionsSketchAggregationV2Protocol.ComputationStages.validTerminalStages
      UNSPECIFIED, UNRECOGNIZED -> error("protocol not set")
    }
  }

  override fun validInitialStage(protocol: ComputationType, stage: ComputationStage): Boolean =
    stage in getValidInitialStage(protocol)

  override fun validTerminalStage(protocol: ComputationType, stage: ComputationStage): Boolean =
    stage in getValidTerminalStages(protocol)

  override fun validTransition(currentStage: ComputationStage, nextStage: ComputationStage):
    Boolean {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      return nextStage in when (currentStage.stageCase) {
        ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1 ->
          LiquidLegionsSketchAggregationV1Protocol.ComputationStages.validSuccessors
        ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
          LiquidLegionsSketchAggregationV2Protocol.ComputationStages.validSuccessors
        ComputationStage.StageCase.STAGE_NOT_SET -> error("Stage not set")
      }.getOrDefault(currentStage, setOf())
    }
}
