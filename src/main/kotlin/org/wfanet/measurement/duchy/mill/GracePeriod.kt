/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill

import java.time.Clock
import java.time.Duration
import java.time.Instant

/**
 * Helper for tracking grace period.
 *
 * This is not thread safe.
 */
class GracePeriod(private val duration: Duration, private val clock: Clock = Clock.systemUTC()) {
  private var startTime: Instant? = null
  private var attempt: Int = 1

  fun start() {
    startTime = clock.instant()
    attempt = 1
  }

  fun reset() {
    startTime = null
    attempt = 1
  }

  fun getAndIncrementAttempt(): Int {
    return attempt++
  }

  val started: Boolean
    get() = startTime != null

  val ended: Boolean
    get() {
      val currentStartTime = startTime ?: return false
      val endTime: Instant = currentStartTime.plus(duration)
      val now: Instant = clock.instant()
      return now.isAfter(endTime)
    }
}
