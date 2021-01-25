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

/** Provides methods for working with an enum representation of stages for a MPC protocol */
interface ProtocolStageEnumHelper<StageT> {
  val validInitialStages: Set<StageT>
  /** True if a computation may start in the given stage. */
  fun validInitialStage(stage: StageT): Boolean {
    return stage in validInitialStages
  }
  val validTerminalStages: Set<StageT>
  fun validTerminalStage(stage: StageT): Boolean = stage in validTerminalStages

  val validSuccessors: Map<StageT, Set<StageT>>
  /** True if a computation may progress from the [currentStage] to the
   * [nextStage].*/
  fun validTransition(
    currentStage: StageT,
    nextStage: StageT
  ): Boolean {
    return nextStage in validSuccessors.getOrDefault(currentStage, setOf())
  }

  /** Turns an enum of type [StageT] into a [Long]. */
  fun enumToLong(value: StageT): Long

  /** Turns a [Long] into an enum of type [StageT]. */
  fun longToEnum(value: Long): StageT
}
