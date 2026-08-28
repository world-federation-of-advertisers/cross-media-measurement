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

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertEquals
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
@OptIn(ExperimentalCoroutinesApi::class) // For `UnconfinedTestDispatcher`.
class RateLimiterThrottlerTest {
  @Test
  fun `onReady does not run block until a permit is acquired`() =
    runTest(UnconfinedTestDispatcher()) {
      val throttler = RateLimiterThrottler(RateLimiter.Blocked)
      var blockRan = false

      val job = launch { throttler.onReady { blockRan = true } }

      assertThat(blockRan).isFalse()
      job.cancel()
    }

  @Test
  fun `onReady returns the result of block`() =
    runTest(UnconfinedTestDispatcher()) {
      val throttler = RateLimiterThrottler(RateLimiter.Unlimited)

      val result = throttler.onReady { "value" }

      assertEquals("value", result)
    }

  @Test
  fun `onReady does not serialize concurrent callers behind an in-flight block`() =
    runTest(UnconfinedTestDispatcher()) {
      // Unlike MinimumIntervalThrottler, RateLimiterThrottler only holds the rate limiter across
      // permit acquisition, not across execution of `block`. A second caller that can acquire a
      // permit should be able to run while a first caller's block is still in flight.
      val throttler = RateLimiterThrottler(RateLimiter.Unlimited)
      val order = mutableListOf<String>()
      val m = Mutex(locked = true)

      val job1 = launch { throttler.onReady { m.withLock { order.add("job1") } } }
      val job2 = launch { throttler.onReady { order.add("job2") } }

      assertThat(order).containsExactly("job2")

      m.unlock()
      job1.join()
      job2.join()
      assertThat(order).containsExactly("job2", "job1").inOrder()
    }
}
