package org.wfanet.measurement.common

interface Throttler {
  /**
   * Determine if the current attempt to do something should be throttled.
   *
   * When this returns true, this assumes an attempt is made. When it returns false, it assumes an
   * attempt hasn't been made.
   *
   * @return true if operation may proceed unthrottled; false if throttled.
   */
  fun attempt(): Boolean

  /**
   * Indicate that an operation encountered pushback.
   */
  fun reportThrottled()

  /**
   * Helper for performing an operation after waiting to be unthrottled.
   *
   * Blocks until ready, then calls [block]. If [block] raises a [ThrottledException], it calls
   * [reportThrottled] and re-throws the exception's cause.
   *
   * @param[block] what to do when not throttled
   */
  suspend fun onReady(block: suspend () -> Unit)
}
