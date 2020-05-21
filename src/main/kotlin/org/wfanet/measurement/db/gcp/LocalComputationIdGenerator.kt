package org.wfanet.measurement.db.gcp

import java.time.Clock

/** Knows how to make local identifiers for computations to be used in a Spanner database. */
interface LocalComputationIdGenerator {
  /** Generates a local identifier for a computation.*/
  fun localId(globalId: Long): Long
}

/** Combines the upper 32 bits of the global id with the lower 32 bits of the current time.  */
class HalfOfGlobalBitsAndTimeStampIdGenerator(private val clock: Clock = Clock.systemUTC()) :
  LocalComputationIdGenerator {
  override fun localId(globalId: Long): Long {
    // TODO: Reverse the bits.
    return (globalId ushr 32) or (clock.millis() shl 32)
  }
}
