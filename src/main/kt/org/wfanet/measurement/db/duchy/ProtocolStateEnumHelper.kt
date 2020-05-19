package org.wfanet.measurement.db.duchy

/** Provides methods for working with an enum representation of states for a MPC protocol */
interface ProtocolStateEnumHelper<T : Enum<T>> {
  val validInitialStates: Set<T>
  /** True if a computation may start in the given stage. */
  fun validInitialState(state: T): Boolean {
    return state in validInitialStates
  }

  val validSuccessors: Map<T, Set<T>>
  /** True if a computation may progress from the [currentState] to the
   * [nextState].*/
  fun validTransition(
    currentState: T,
    nextState: T
  ): Boolean {
    return nextState in validSuccessors.getOrDefault(currentState, setOf())
  }

  /** Turns an enum of type [T] into a [Long]. */
  fun enumToLong(value: T): Long

  /** Turns a [Long] into an enum of type [T]. */
  fun longToEnum(value: Long): T
}
