package org.wfanet.measurement.duchy.db.computationstat

/** Database interface for `ComputationStat` resources. */
interface ComputationStatDatabase {

  /**
   * Inserts the specified [ComputationStat] into the database.
   */
  suspend fun insertComputationStat(
    localId: Long,
    stage: Long,
    attempt: Long,
    metricName: String,
    globalId: String,
    role: String,
    isSuccessfulAttempt: Boolean,
    value: Long
  )
}
