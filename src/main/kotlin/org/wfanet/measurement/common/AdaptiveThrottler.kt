// TODO(efoxepstein): add tests

package org.wfanet.measurement.common

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.ArrayDeque
import kotlin.random.Random
import kotlinx.coroutines.delay

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
 * @param[pollDelayMillis] how often to retry when throttled in [onReady]
 */
class AdaptiveThrottler(
  private val overloadFactor: Double,
  private val clock: Clock,
  private val timeHorizon: Duration,
  private val pollDelayMillis: Long
) : Throttler {
  private val requests = ArrayDeque<Instant>()
  private val rejects = ArrayDeque<Instant>()

  private val numRequests: Int
    get() = requests.size

  private val numAccepts: Int
    get() = numRequests - rejects.size

  private fun rejectionProbability(): Double =
    (numRequests - overloadFactor * numAccepts) / (numRequests + 1)

  override fun attempt(): Boolean =
    (Random.nextDouble() > rejectionProbability()).also {
      if (it) {
        updateQueue(requests)
      }
    }

  override fun reportThrottled() {
    updateQueue(rejects)
  }

  override suspend fun onReady(block: suspend () -> Unit) {
    while (!attempt()) {
      delay(pollDelayMillis)
    }
    try {
      block()
    } catch (e: ThrottledException) {
      reportThrottled()
      e.cause?.let { throw it }
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
