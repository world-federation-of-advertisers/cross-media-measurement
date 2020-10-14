// Copyright 2020 The Measurement System Authors
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
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * A throttler to ensure the minimum interval between actions is at least some [Duration].
 *
 * @property[clock] the clock to use
 * @property[interval] the minimum interval between events
 */
class MinimumIntervalThrottler(
  private val clock: Clock,
  private val interval: Duration
) : Throttler {
  private val mutex = Mutex(false) // Guarantees FIFO order.
  private var lastAttempt = Instant.EPOCH

  /**
   * Runs [block] once [interval] passed since the last [onReady] call finished.
   *
   * Note that this deliberately locks the throttler until [block] completes and offers FIFO
   * semantics for concurrent calls to onReady.
   *
   * DEADLOCK WARNING: do not call [onReady] from [block]!
   */
  override suspend fun <T> onReady(block: suspend () -> T): T {
    mutex.withLock {
      val nextAttempt = lastAttempt.plus(interval)
      while (true) {
        val delta = Duration.between(nextAttempt, clock.instant())
        if (delta > Duration.ZERO) {
          break
        }
        delay(delta.toMillis())
      }
      lastAttempt = clock.instant()
      return block()
    }
  }
}
