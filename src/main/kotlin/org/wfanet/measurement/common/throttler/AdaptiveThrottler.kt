// Copyright 2020 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.common.throttler

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.ArrayDeque
import kotlin.properties.Delegates
import kotlin.random.Random
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import picocli.CommandLine

/**
 * Provides an adaptive throttler.
 *
 * This throttler tracks the number of attempts and failures over the last [timeHorizon] time window
 * to dynamically delay calls to [onReady] to reduce throttling while maximizing throughput.
 *
 * For most batch use cases, [overloadFactor] should be 1.1.
 * For interactive use cases, a higher value is acceptable (e.g. 2.0).
 *
 * @param overloadFactor how much to overload the backend
 * @param clock a clock
 * @param timeHorizon what time window to look at to determine proportion of accepted requests
 * @param pollDelay how often to retry when throttled in [onReady]
 */
class AdaptiveThrottler(
  private val overloadFactor: Double,
  private val clock: Clock,
  private val timeHorizon: Duration,
  private val pollDelay: Duration
) : Throttler {

  /** Create an [AdaptiveThrottler] from the values set in [Flags]. */
  constructor(flags: Flags) : this(
    flags.overloadFactor,
    Clock.systemUTC(),
    flags.timeHorizon,
    flags.throttlePollDelay
  )

  private val mutex = Mutex(false)
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

  private suspend fun updateQueue(queue: ArrayDeque<Instant>) {
    mutex.withLock {
      val now = clock.instant()
      queue.offer(now)
      while (Duration.between(queue.peek(), now) > timeHorizon) {
        queue.poll()
      }
    }
  }

  /** Command line flags to create an adaptive throttler. */
  class Flags {

    @set:CommandLine.Option(
      names = ["--throttler-overload-factor"],
      required = true,
      description = [
        "How much to overload the backend. If the factor is 1.1 it is expected ",
        "that 10% of requests will fail due to throttling."
      ],
      defaultValue = "1.1"
    )
    var overloadFactor by Delegates.notNull<Double>()
      private set

    @CommandLine.Option(
      names = ["--throttler-time-horizon"],
      required = true,
      description = ["Time window to look at to determine proportion of accepted requests."],
      defaultValue = "2m"
    )
    lateinit var timeHorizon: Duration
      private set

    @CommandLine.Option(
      names = ["--throttler-poll-delay"],
      required = true,
      description = ["How often to retry when throttled."],
      defaultValue = "1ms"
    )
    lateinit var throttlePollDelay: Duration
      private set
  }
}
