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

/** Provides methods for working with an enum representation of stages for MPC protocols */
interface ComputationProtocolStagesEnumHelper<ProtocolT, StageT> {

  /** Gets the protocol type from a stage type. */
  fun stageToProtocol(stage: StageT): ProtocolT

  /** Turns an enum of [StageT] into a [ComputationStageLongValues]. */
  fun computationStageEnumToLongValues(value: StageT): ComputationStageLongValues

  /** Turns a [ComputationStageLongValues] into an enum of [StageT]. */
  fun longValuesToComputationStageEnum(value: ComputationStageLongValues): StageT

  /** Returns the set of valid initial stages for [protocol]. */
  fun getValidInitialStage(protocol: ProtocolT): Set<StageT>

  /** Returns the set of valid terminal stages for [protocol]. */
  fun getValidTerminalStages(protocol: ProtocolT): Set<StageT>

  /** True if a [protocol] computation may start in the given stage. */
  fun validInitialStage(protocol: ProtocolT, stage: StageT): Boolean

  /** True if a [protocol] computation may end in the given stage. */
  fun validTerminalStage(protocol: ProtocolT, stage: StageT): Boolean

  /** True if a computation may progress from the [currentStage] to the [nextStage].*/
  fun validTransition(currentStage: StageT, nextStage: StageT): Boolean
}

data class ComputationStageLongValues(
  val protocol: Long,
  val stage: Long
)
