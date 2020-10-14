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

import com.google.common.collect.Range
import com.google.common.truth.Truth.assertThat
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runBlockingTest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
@OptIn(ExperimentalCoroutinesApi::class) // For `runBlockingTest`.
class AdaptiveThrottlerTest {
  @Test
  fun onReady() = runBlockingTest {
    val clock = object : Clock() {
      override fun instant(): Instant = Instant.ofEpochMilli(currentTime)
      override fun withZone(zone: ZoneId?): Clock = error("Unimplemented")
      override fun getZone(): ZoneId = error("Unimplemented")
    }
    val throttler = AdaptiveThrottler(
      overloadFactor = 2.0,
      clock = clock,
      timeHorizon = Duration.ofSeconds(1L),
      pollDelay = Duration.ofMillis(1L)
    )

    val events = mutableListOf<Int>()

    class TestException(message: String) : Exception(message)

    // Accept at most 100qps and ensure that the throttler converges on that.
    val begin = clock.instant()
    var lastAccept = Instant.EPOCH
    var numExceptions = 0
    repeat(10_000) { i ->
      try {
        throttler.onReady {
          events.add(i)
          if (Duration.between(lastAccept, clock.instant()) < Duration.ofMillis(10)) {
            throw ThrottledException("some message $i", TestException("Uh oh $i"))
          }
          lastAccept = clock.instant()
        }
      } catch (e: TestException) {
        // Deliberately suppressed.
        numExceptions++
      }
    }

    // Since the overload factor is 2, we expect around half of the events to throttle.
    assertThat(numExceptions).isIn(4500..5500)

    // Should converge around 100qps (and thus take 10,000 / 100 = 100 seconds).
    // However, since there's randomness involved, we accept a wide range of durations.
    val testDuration = Duration.between(begin, lastAccept)
    assertThat(testDuration)
      .isIn(Range.open(Duration.ofSeconds(90), Duration.ofSeconds(110)))

    // Ensure all the events actually executed.
    assertThat(events)
      .containsExactlyElementsIn(0 until 10_000)
      .inOrder()
  }
}
