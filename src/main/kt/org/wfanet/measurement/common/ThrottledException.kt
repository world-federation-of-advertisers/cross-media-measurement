package org.wfanet.measurement.common

/**
 * Indicates pushback used for throttling.
 */
class ThrottledException(
  message: String,
  throwable: Throwable? = null
) : Exception(
  message,
  throwable
)
