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

import com.google.common.truth.FailureMetadata
import com.google.common.truth.MapSubject
import com.google.common.truth.Truth.assertAbout

typealias RelativeFrequencyDistribution = Map<Long, Double>

class FrequencyDistributionSubject
private constructor(failureMetadata: FailureMetadata, subject: RelativeFrequencyDistribution) :
  MapSubject(failureMetadata, subject) {

  private val actual: RelativeFrequencyDistribution = subject

  fun isWithin(tolerance: Double): DistributionComparison {
    return object : DistributionComparison() {
      override fun of(expected: RelativeFrequencyDistribution, maxFrequency: Long) {
        for (bucket in 1L..maxFrequency) {
          val actualValue = actual.getOrDefault(bucket, 0.0)
          val expectedValue = expected.getOrDefault(bucket, 0.0)
          check("getValue($bucket)").that(actualValue).isWithin(tolerance).of(expectedValue)
        }
      }
    }
  }

  abstract class DistributionComparison internal constructor() {
    abstract fun of(expected: RelativeFrequencyDistribution, maxFrequency: Long)
  }

  companion object {
    fun frequencyDistributions():
      (
        failureMetadata: FailureMetadata, subject: RelativeFrequencyDistribution
      ) -> FrequencyDistributionSubject = ::FrequencyDistributionSubject

    fun assertThat(subject: RelativeFrequencyDistribution): FrequencyDistributionSubject =
      assertAbout(frequencyDistributions()).that(subject)
  }
}
