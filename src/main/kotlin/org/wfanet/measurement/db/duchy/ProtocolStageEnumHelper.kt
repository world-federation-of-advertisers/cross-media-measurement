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

package org.wfanet.measurement.db.duchy

/** Provides methods for working with an enum representation of stages for a MPC protocol */
interface ProtocolStageEnumHelper<T> {
  val validInitialStages: Set<T>
  /** True if a computation may start in the given stage. */
  fun validInitialStage(stage: T): Boolean {
    return stage in validInitialStages
  }
  val validTerminalStages: Set<T>
  fun validTerminalStage(stage: T): Boolean = stage in validTerminalStages

  val validSuccessors: Map<T, Set<T>>
  /** True if a computation may progress from the [currentStage] to the
   * [nextStage].*/
  fun validTransition(
    currentStage: T,
    nextStage: T
  ): Boolean {
    return nextStage in validSuccessors.getOrDefault(currentStage, setOf())
  }

  /** Turns an enum of type [T] into a [Long]. */
  fun enumToLong(value: T): Long

  /** Turns a [Long] into an enum of type [T]. */
  fun longToEnum(value: Long): T
}
