/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common

import com.google.common.truth.Truth.assertThat
import java.time.Duration
import kotlin.random.Random
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

/** Test for [ExponentialBackoff]. */
@RunWith(JUnit4::class)
class ExponentialBackoffTest {
  @Test
  fun `durationForAttempt returns initial delay for attempt 1`() {
    val backoff = ExponentialBackoff(initialDelay = Duration.ofMillis(250), randomnessFactor = 0.0)

    val duration: Duration = backoff.durationForAttempt(1)

    assertThat(duration).isEqualTo(backoff.initialDelay)
  }

  @Test
  fun `durationForAttempt returns multiplied value with no randomness factor`() {
    val backoff =
      ExponentialBackoff(
        initialDelay = Duration.ofMillis(500),
        multiplier = 2.0,
        randomnessFactor = 0.0,
      )

    val duration: Duration = backoff.durationForAttempt(4)

    assertThat(duration).isEqualTo(Duration.ofMillis(4000))
  }

  @Test
  fun `durationForAttempt returns multiplied value with randomness factor`() {
    val backoff =
      ExponentialBackoff(
        initialDelay = Duration.ofMillis(500),
        multiplier = 2.0,
        randomnessFactor = 0.5,
        random = Random(RANDOM_SEED),
      )

    val duration: Duration = backoff.durationForAttempt(4)

    assertThat(duration).isEqualTo(Duration.ofMillis(2559))
  }

  @Test
  fun `durationForAttempt throws IllegalArgumentException for invalid attempt number`() {
    val backoff = ExponentialBackoff()

    val exception = assertFailsWith<IllegalArgumentException> { backoff.durationForAttempt(0) }

    assertThat(exception).hasMessageThat().ignoringCase().contains("attempt")
  }

  @Test
  fun `init throws IllegalArgumentException for invalid randomness factor`() {
    val exception =
      assertFailsWith<IllegalArgumentException> { ExponentialBackoff(randomnessFactor = -0.5) }

    assertThat(exception).hasMessageThat().ignoringCase().contains("randomness")
  }

  @Test
  fun `init throws IllegalArgumentException for invalid initial delay`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ExponentialBackoff(initialDelay = Duration.ofNanos(1))
      }

    assertThat(exception).hasMessageThat().ignoringCase().contains("delay")
  }

  companion object {
    private const val RANDOM_SEED = 1L
  }
}
