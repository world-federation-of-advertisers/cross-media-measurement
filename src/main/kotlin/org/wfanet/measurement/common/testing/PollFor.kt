package org.wfanet.measurement.common.testing

import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout

/**
 * Repeatedly invokes [producer] until it returns something non-null.
 *
 * @param timeoutMillis the maximum amount of time to wait for a non-null value
 * @param delayMillis the time between calling [producer]
 * @param producer block that produces a [T] or null
 */
suspend fun <T> pollFor(
  timeoutMillis: Long = 3_000L,
  delayMillis: Long = 250L,
  producer: suspend () -> T?
): T {
  return withTimeout<T>(timeoutMillis) {
    var t: T? = producer()
    while (t == null) {
      delay(delayMillis)
      t = producer()
    }
    t
  }
}
