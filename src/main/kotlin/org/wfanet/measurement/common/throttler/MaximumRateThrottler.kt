/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.throttler

import kotlin.time.ComparableTimeMark
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * [Throttler] which limits executions to a maximum rate.
 *
 * This is conceptually similar to [MinimumIntervalThrottler] but uses [TimeSource] and allows
 * concurrent executions.
 */
class MaximumRateThrottler
private constructor(maxPerSecond: Double, private val timeSource: TimeSource.WithComparableMarks) :
  Throttler {
  init {
    require(maxPerSecond > 0)
  }

  /**
   * Constructs a new [MaximumRateThrottler].
   *
   * @param maxPerSecond Maximum number of executions allowed per second
   */
  constructor(maxPerSecond: Double) : this(maxPerSecond, TimeSource.Monotonic)

  private val minTimeBetweenExecutions: Duration = (1 / maxPerSecond).seconds
  private val mutex = Mutex()

  /** Time of last execution, guarded by [mutex]. */
  private var lastExecution: ComparableTimeMark? = null

  private suspend inline fun <R> sinceLastExecution(action: (Duration) -> R): R {
    return mutex.withLock { action(lastExecution?.elapsedNow() ?: Duration.INFINITE) }
  }

  private suspend fun delayUntilExecutionAllowed() {
    // Delay in a loop to account for the fact that there may be concurrent executions.
    do {
      val delayDuration: Duration = sinceLastExecution { duration ->
        if (duration < minTimeBetweenExecutions) {
          minTimeBetweenExecutions - duration
        } else {
          Duration.ZERO
        }
      }

      // Delay outside the mutex to allow concurrent executions.
      delay(delayDuration)
    } while (sinceLastExecution { it < minTimeBetweenExecutions })
  }

  private suspend fun updateLastExecution() =
    mutex.withLock { this.lastExecution = timeSource.markNow() }

  override suspend fun <T> onReady(block: suspend () -> T): T {
    delayUntilExecutionAllowed()
    updateLastExecution()
    return block()
  }

  companion object {
    fun forTesting(maxPerSecond: Double, timeSource: TimeSource.WithComparableMarks) =
      MaximumRateThrottler(maxPerSecond, timeSource)
  }
}
