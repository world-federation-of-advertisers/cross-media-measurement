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

import java.time.Duration
import kotlin.math.pow
import kotlin.random.Random

/**
 * Exponential backoff durations at millisecond resolution.
 *
 * TODO(@SanjayVas): Move this to common-jvm.
 */
data class ExponentialBackoff(
  val initialDelay: Duration = Duration.ofMillis(500),
  val multiplier: Double = 1.5,
  val randomnessFactor: Double = 0.5,
  val random: Random = Random,
) {
  init {
    require(initialDelay >= MIN_INITIAL_DELAY) {
      "Initial delay must be at least $MIN_INITIAL_DELAY"
    }
    require(randomnessFactor >= 0.0) { "Randomness factor may not be negative" }
  }

  /** Returns the [Duration] for the specified [attemptNumber], starting at `1`. */
  fun durationForAttempt(attemptNumber: Int): Duration {
    return random.randomizedDuration(
      exponentialDuration(
        initialDelay = initialDelay,
        multiplier = multiplier,
        attemptNumber = attemptNumber,
      ),
      randomnessFactor = randomnessFactor,
    )
  }

  companion object {
    /** Minimum initial delay. */
    val MIN_INITIAL_DELAY: Duration = Duration.ofMillis(1)

    private fun exponentialDuration(
      initialDelay: Duration,
      multiplier: Double,
      attemptNumber: Int,
    ): Duration {
      require(attemptNumber > 0) { "Attempts start at 1" }
      if (attemptNumber == 1) {
        // Minor optimization to avoid unnecessary floating point math.
        return initialDelay
      }
      return Duration.ofMillis(
        (initialDelay.toMillis() * (multiplier.pow(attemptNumber - 1))).toLong()
      )
    }

    private fun Random.randomizedDuration(delay: Duration, randomnessFactor: Double): Duration {
      if (randomnessFactor == 0.0) {
        return delay
      }
      val maxOffset = randomnessFactor * delay.toMillis()
      return delay.plusMillis(nextDouble(-maxOffset, maxOffset).toLong())
    }
  }
}

fun Duration.coerceAtMost(maximumValue: Duration): Duration {
  return if (this <= maximumValue) this else maximumValue
}
