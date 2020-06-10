package org.wfanet.measurement.db.duchy

/** Deals with stage specific details for a computation protocol. */
interface ProtocolStageDetails<T: Enum<T>, StageDetailsT> {
  /** Creates the stage specific details for a given computation stage. */
  fun detailsFor(stage: T) : StageDetailsT

  /** Converts bytes into a [StageDetailsT] .*/
  fun parseDetails(bytes: ByteArray): StageDetailsT
}
