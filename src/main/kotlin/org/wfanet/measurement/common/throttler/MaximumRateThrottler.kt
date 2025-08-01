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

import kotlin.time.TimeSource
import org.wfanet.measurement.common.ratelimit.TokenBucket

/**
 * [Throttler] which limits executions to a maximum rate.
 *
 * This is conceptually similar to [MinimumIntervalThrottler] but uses [TimeSource] and allows
 * concurrent executions.
 */
class MaximumRateThrottler(
  maxPerSecond: Double,
  timeSource: TimeSource.WithComparableMarks = TimeSource.Monotonic,
) : Throttler {
  init {
    require(maxPerSecond > 0)
  }

  private val rateLimiter = TokenBucket(1, maxPerSecond, timeSource)

  suspend fun acquire() = rateLimiter.acquire()

  override suspend fun <T> onReady(block: suspend () -> T): T {
    acquire()
    return block()
  }
}
