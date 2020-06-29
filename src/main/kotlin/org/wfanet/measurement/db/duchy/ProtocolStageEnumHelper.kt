package org.wfanet.measurement.db.duchy

/** Provides methods for working with an enum representation of stages for a MPC protocol */
interface ProtocolStageEnumHelper<T : Enum<T>> {
  val validInitialStages: Set<T>
  /** True if a computation may start in the given stage. */
  fun validInitialStage(stage: T): Boolean {
    return stage in validInitialStages
  }

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
