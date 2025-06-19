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

import com.google.common.truth.Truth.assertThat
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class MaximumRateThrottlerTest {
  @Test
  fun `onReady throttled to maximum rate`() = runTest {
    val throttler = MaximumRateThrottler(5.0, testScheduler.timeSource)
    val count = AtomicInteger()

    val job: Job = launch {
      while (coroutineContext.isActive) {
        throttler.onReady { count.incrementAndGet() }
      }
    }
    testScheduler.advanceTimeBy(3.seconds)
    job.cancelAndJoin()

    // Due to floating point math this can be off by 1.
    assertThat(count.get()).isWithin(1).of(15)
  }

  @Test
  fun `onReady throttled to maximum rate from multiple coroutines`() = runTest {
    val throttler = MaximumRateThrottler(5.0, testScheduler.timeSource)
    val count = AtomicInteger()

    val job1: Job = launch {
      while (coroutineContext.isActive) {
        throttler.onReady { count.incrementAndGet() }
      }
    }
    val job2: Job = launch {
      while (coroutineContext.isActive) {
        throttler.onReady { count.incrementAndGet() }
      }
    }
    testScheduler.advanceTimeBy(3.seconds)
    job1.cancelAndJoin()
    job2.cancelAndJoin()

    // Due to floating point math this can be off by 1.
    assertThat(count.get()).isWithin(1).of(15)
  }
}
