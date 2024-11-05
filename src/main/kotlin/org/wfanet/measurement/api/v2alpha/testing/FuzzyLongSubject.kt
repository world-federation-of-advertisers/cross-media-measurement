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

package org.wfanet.measurement.api.v2alpha.testing

import com.google.common.truth.Fact
import com.google.common.truth.FailureMetadata
import com.google.common.truth.LongSubject
import kotlin.math.abs

/**
 * [LongSubject] that allows fuzzy comparisons.
 *
 * TODO(@SanjayVas): Move this to common-jvm.
 */
class FuzzyLongSubject private constructor(failureMetadata: FailureMetadata, subject: Long?) :
  LongSubject(failureMetadata, subject) {

  private val actual: Long? = subject

  fun isWithinPercent(percent: Double): LongRatioComparison {
    return object : LongRatioComparison() {
      override fun of(expected: Long) {
        if (actual == null) {
          failWithActual(Fact.simpleFact("expected not to be null"))
          return
        }

        val actualRatio: Double = abs(expected - actual) / expected.toDouble()
        val expectedRatio: Double = percent / 100.0
        if (actualRatio > expectedRatio) {
          failWithActual(Fact.fact("expected to be within", "$percent%"), Fact.fact("of", expected))
        }
      }
    }
  }

  fun isWithin(tolerance: Double): LongRatioComparison {
    return object : LongRatioComparison() {
      override fun of(expected: Long) {
        if (actual == null) {
          failWithActual(Fact.simpleFact("expected not to be null"))
          return
        }

        check("getValue").that(actual.toDouble()).isWithin(tolerance).of(expected.toDouble())
      }
    }
  }

  abstract class LongRatioComparison internal constructor() {
    abstract fun of(expected: Long)
  }

  companion object {
    fun fuzzyLongs() =
      Factory<FuzzyLongSubject, Long> { metadata, actual -> FuzzyLongSubject(metadata, actual) }
  }
}
