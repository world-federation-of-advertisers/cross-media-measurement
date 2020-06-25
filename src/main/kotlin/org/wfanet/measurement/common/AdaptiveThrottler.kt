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
 * to dynamically delay calls to [onReady] to reduce throttling while maximizing throughput.
 *
 * For most batch use cases, [overloadFactor] should be 1.1.
 * For interactive use cases, a higher value is acceptable (e.g. 2.0).
 *
 * @param[overloadFactor] how much to overload the backend
 * @param[clock] a clock
 * @param[timeHorizon] what time window to look at to determine proportion of accepted requests
 * @param[pollDelay] how often to retry when throttled in [onReady]
 */
class AdaptiveThrottler(
  private val overloadFactor: Double,
  private val clock: Clock,
  private val timeHorizon: Duration,
  private val pollDelay: Duration
) : Throttler {

  private val requests = ArrayDeque<Instant>()
  private val accepts = ArrayDeque<Instant>()

  private val numRequests: Int
    get() = requests.size

  private val numAccepts: Int
    get() = accepts.size

  private val rejectionProbability: Double
    get() = (numRequests - overloadFactor * numAccepts) / (numRequests + 1)

  /**
   * Repeatedly delays by [pollDelay] and then runs and returns the result of [block].
   *
   * The total amount of delay converges so that the resources accessed by [block] are overloaded by
   * [overloadFactor]. For example, if [overloadFactor] is 1.1, we expect 10% of requests to throw
   * a [ThrottledException].
   */
  override suspend fun <T> onReady(block: suspend () -> T): T {
    while (Random.nextDouble() < rejectionProbability) {
      updateQueue(requests)
      delay(pollDelay.toMillis())
    }
    updateQueue(requests)
    val result = try {
      block()
    } catch (e: ThrottledException) {
      throw checkNotNull(e.cause) { "ThrottledException thrown without cause: $e" }
    }
    updateQueue(accepts)
    return result
  }

  private fun updateQueue(queue: ArrayDeque<Instant>) {
    val now = clock.instant()
    queue.offer(now)
    while (Duration.between(queue.peek(), now) > timeHorizon) {
      queue.poll()
    }
  }
}
