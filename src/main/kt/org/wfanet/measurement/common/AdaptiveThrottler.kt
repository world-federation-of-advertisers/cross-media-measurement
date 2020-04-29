// TODO(efoxepstein): add tests

package org.wfanet.measurement.common

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.ArrayDeque
import kotlin.random.Random

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

  fun attempt(): Boolean =
    (Random.nextDouble() > rejectionProbability()).also {
      if (it) {
        updateQueue(requests)
      }
    }

  fun reportThrottled() {
    updateQueue(rejects)
  }

  private fun updateQueue(queue: ArrayDeque<Instant>) {
    val now = clock.instant()
    queue.offer(now)
    if (Duration.between(queue.peek(), now) < timeHorizon) {
      queue.poll()
    }
  }

}