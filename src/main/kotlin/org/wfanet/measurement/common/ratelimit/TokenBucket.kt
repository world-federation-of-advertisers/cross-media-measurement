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

import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.coroutineContext
import kotlin.math.floor
import kotlin.time.ComparableTimeMark
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource
import kotlinx.coroutines.CompletableJob
import kotlinx.coroutines.Job
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay

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
  private val tokenCount = AtomicInteger(size)

  /** Time of last refill, guarded by `this`. */
  @Volatile private var lastRefill: ComparableTimeMark = timeSource.markNow()

  /** Queue of [Acquirer]s for FIFO ordering, guarded by `this`. */
  @Volatile private var acquirers = ArrayDeque<Acquirer>()

  override fun tryAcquire(permitCount: Int): Boolean {
    require(permitCount >= 0)

    if (permitCount > size) {
      return false
    }

    return synchronized(this) {
      refill()
      tryConsumeTokens(permitCount)
    }
  }

  override suspend fun acquire(permitCount: Int) {
    require(permitCount >= 0)

    if (permitCount > size) {
      awaitCancellation()
    }

    val acquirer = Acquirer(permitCount, Job(coroutineContext[Job]))
    synchronized(this) { acquirers.add(acquirer) }
    while (acquirer.job.isActive) {
      refill()

      val tokensNeeded = permitCount - tokenCount.get()
      if (tokensNeeded > 0) {
        delay(refillTime * tokensNeeded)
      }
    }
    acquirer.job.join()
  }

  /**
   * Attempts to consume [count] tokens.
   *
   * @return whether tokens were consumed, i.e. whether there were enough tokens to consume
   */
  private fun tryConsumeTokens(count: Int): Boolean {
    val beforeCount =
      tokenCount.getAndUpdate { currentCount ->
        if (currentCount >= count) {
          currentCount - count
        } else {
          currentCount
        }
      }

    return beforeCount >= count
  }

  @Synchronized
  private fun refill() {
    // Add tokens.
    val now: ComparableTimeMark = timeSource.markNow()
    val elapsed = now - lastRefill
    check(!elapsed.isNegative()) { "Time source is non-monotonic" }
    val tokensToAdd = floor(elapsed / refillTime).toInt()
    if (tokensToAdd > 0) {
      tokenCount.updateAndGet { count -> (count + tokensToAdd).coerceAtMost(size) }
      lastRefill = now
    }

    releaseAcquirers()
  }

  @Synchronized
  private fun releaseAcquirers() {
    while (acquirers.isNotEmpty()) {
      val acquirer: Acquirer = acquirers.first() // Peek.

      if (tryConsumeTokens(acquirer.permitCount)) {
        acquirers.removeFirst() // Dequeue.
        acquirer.job.complete()
      } else {
        break
      }
    }
  }

  private data class Acquirer(val permitCount: Int, val job: CompletableJob)
}
