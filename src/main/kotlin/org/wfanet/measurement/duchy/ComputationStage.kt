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

package org.wfanet.measurement.duchy

import org.wfanet.measurement.api.v1alpha.GlobalComputationStatusUpdate.MpcAlgorithm
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.duchy.ComputationStage

val ComputationStage.name: String
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  get() = when (stageCase) {
    ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION ->
      liquidLegionsSketchAggregation.name
    ComputationStage.StageCase.STAGE_NOT_SET -> error("Stage not set")
  }

val ComputationStage.mpcAlgorithm: MpcAlgorithm
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  get() = when (stageCase) {
    ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION ->
      MpcAlgorithm.LIQUID_LEGIONS
    ComputationStage.StageCase.STAGE_NOT_SET -> error("Stage not set")
  }

val ComputationStage.number: Int
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  get() = when (stageCase) {
    ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION ->
      liquidLegionsSketchAggregation.number
    ComputationStage.StageCase.STAGE_NOT_SET -> error("Stage not set")
  }

fun LiquidLegionsSketchAggregationStage.toProtocolStage(): ComputationStage =
  ComputationStage.newBuilder().setLiquidLegionsSketchAggregation(this).build()
