/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common

import java.time.LocalDate
import java.time.Period
import java.time.temporal.TemporalAmount

data class LocalDateProgression(
  override val start: LocalDate,
  override val endInclusive: LocalDate,
  val step: TemporalAmount = Period.ofDays(1)
) : ClosedRange<LocalDate>, Iterable<LocalDate> {
  override fun iterator(): Iterator<LocalDate> {
    return iterator {
      var current = start
      while (current <= endInclusive) {
        yield(current)
        current = current.plus(step)
      }
    }
  }
}

operator fun LocalDate.rangeTo(that: LocalDate) = LocalDateProgression(this, that)
