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

package org.wfanet.measurement.common.ratelimit

import kotlin.math.floor
import kotlin.time.ComparableTimeMark
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * Implementation of the token bucket rate limit algorithm.
 *
 * @param size size of the bucket
 * @param fillRate number of tokens filled per second
 */
class TokenBucket(
  private val size: Int,
  fillRate: Double,
  private val timeSource: TimeSource.WithComparableMarks = TimeSource.Monotonic,
) : RateLimiter {
  init {
    require(size >= 0)
    require(fillRate > 0)
  }

  /** Time it takes to refill one token. */
  private val refillTime: Duration = (1 / fillRate).seconds
  @Volatile private var tokenCount: Int = size
  @Volatile private var lastRefill: ComparableTimeMark = timeSource.markNow()

  @Synchronized
  override fun tryAcquire(cost: Int): RateLimiter.Permit? {
    require(cost >= 0)

    if (cost > size) {
      return null
    }

    refill()
    return if (tokenCount >= cost) {
      tokenCount -= cost
      RateLimiter.Permit.NoOp // Token bucket algorithm does not track release of permits.
    } else {
      null
    }
  }

  @Synchronized
  private fun refill() {
    val now: ComparableTimeMark = timeSource.markNow()

    val elapsed = now - lastRefill
    check(!elapsed.isNegative()) { "Time source is non-monotonic" }
    val tokensToAdd = floor(elapsed / refillTime).toInt()
    if (tokensToAdd == 0) {
      return
    }

    if (tokenCount + tokensToAdd >= size) {
      tokenCount = size
    } else {
      tokenCount += tokensToAdd
    }

    lastRefill = now
  }
}
