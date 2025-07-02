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

import kotlinx.coroutines.awaitCancellation

/** Rate limiter. */
interface RateLimiter {
  /**
   * Attempts to acquire permits for execution without suspending.
   *
   * @return whether permits were acquired
   */
  fun tryAcquire(permitCount: Int = 1): Boolean

  /** Suspends until the requested number of permits for execution are acquired. */
  suspend fun acquire(permitCount: Int = 1)

  companion object {
    val Unlimited =
      object : RateLimiter {
        override fun tryAcquire(permitCount: Int) = true

        override suspend fun acquire(permitCount: Int) {}
      }
    val Blocked =
      object : RateLimiter {
        override fun tryAcquire(permitCount: Int) = false

        override suspend fun acquire(permitCount: Int) = awaitCancellation()
      }
  }
}

suspend inline fun <T> RateLimiter.withPermits(permitCount: Int = 1, action: () -> T): T {
  acquire(permitCount)
  return action()
}
