package org.wfanet.measurement.common

interface Throttler {
  /**
   * Helper for performing an operation after waiting to be unthrottled.
   *
   * Blocks until ready, then calls [block]. If [block] raises a [ThrottledException], it calls
   * [reportThrottled] and re-throws the exception's cause.
   *
   * @param[block] what to do when not throttled
   */
  suspend fun <T> onReady(block: suspend () -> T): T
}
