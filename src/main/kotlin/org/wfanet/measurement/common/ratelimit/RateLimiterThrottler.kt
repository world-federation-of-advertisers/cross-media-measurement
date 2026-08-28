/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

import org.wfanet.measurement.common.throttler.Throttler

/**
 * [Throttler] backed by a [RateLimiter].
 *
 * Unlike `MinimumIntervalThrottler`, this only gates when [block] is allowed to *start*:
 * [RateLimiter.acquire] does not hold any lock across execution of [block], so multiple concurrent
 * callers can have [block] in flight at the same time once each has acquired a permit. This keeps
 * concurrent callers (e.g. parallel fulfillment) from being serialized behind each other's RPC
 * latency, while still bounding the aggregate rate at which new calls start.
 */
class RateLimiterThrottler(private val rateLimiter: RateLimiter) : Throttler {
  override suspend fun <T> onReady(block: suspend () -> T): T {
    rateLimiter.acquire()
    return block()
  }
}
