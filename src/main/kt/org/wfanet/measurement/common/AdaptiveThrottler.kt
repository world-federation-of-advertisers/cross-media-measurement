// TODO(efoxepstein): add tests

package org.wfanet.measurement.common

import kotlinx.coroutines.delay
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.ArrayDeque
import kotlin.random.Random

/**
 * Indicates pushback used for throttling.
 */
class ThrottledException(message: String, throwable: Throwable? = null) : Exception(message,
                                                                                    throwable) {}


/**
 * Provides an adaptive throttler.
 *
 * This throttler tracks the number of attempts and failures over the last [timeHorizon] time window
 * and adjusts the proportion of the time that [attempt] returns true so that the rate of attempts
 * is [overloadFactor] times what the backend can handle.
 *
 * @param[overloadFactor] how much to overload the backend
 * @param[clock] a clock
 * @param[timeHorizon] what time window to look at to determine proportion of accepted requests
 */
class AdaptiveThrottler(private val overloadFactor: Double,
                        private val clock: Clock,
                        private val timeHorizon: Duration) {
  private val requests = ArrayDeque<Instant>()
  private val rejects = ArrayDeque<Instant>()

  private val numRequests: Int
    get() = requests.size

  private val numAccepts: Int
    get() = numRequests - rejects.size

  private fun rejectionProbability(): Double =
    (numRequests - overloadFactor * numAccepts) / (numRequests + 1)

  /**
   * Determine if the current attempt to do something should be throttled.
   *
   * When this returns true, this assumes an attempt is made. When it returns false, it assumes an
   * attempt hasn't been made.
   *
   * @return true if operation may proceed unthrottled; false if throttled.
   */
  fun attempt(): Boolean =
    (Random.nextDouble() > rejectionProbability()).also {
      if (it) {
        updateQueue(requests)
      }
    }

  /**
   * Indicate that an operation encountered pushback.
   */
  fun reportThrottled() {
    updateQueue(rejects)
  }

  /**
   * Helper for performing an operation when unthrottled.
   *
   * This repeatedly polls [attempt] at [pollIntervalMillis] intervals until it returns true, then
   * calls [block]. If [block] raises a [ThrottledException], it calls [reportThrottled] and
   * re-throws the exception's cause.
   *
   * @param[pollIntervalMillis] how frequently to retry
   * @param[block] what to do when not throttled
   */
  suspend fun onReady(pollIntervalMillis: Long, block: suspend () -> Unit) {
    while (!attempt()) {
      delay(pollIntervalMillis)
    }
    try {
      block()
    } catch (e: ThrottledException) {
      reportThrottled()
      if (e.cause != null)  throw e.cause
    }
  }

  private fun updateQueue(queue: ArrayDeque<Instant>) {
    val now = clock.instant()
    queue.offer(now)
    if (Duration.between(queue.peek(), now) < timeHorizon) {
      queue.poll()
    }
  }

}