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

import com.google.common.truth.ExpectFailure.assertThat
import com.google.common.truth.ExpectFailure.expectFailureAbout
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.frequency
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementKt.result
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.testing.MeasurementResultSubject.Companion.assertThat
import org.wfanet.measurement.api.v2alpha.testing.MeasurementResultSubject.Companion.measurementResults

@RunWith(JUnit4::class)
class MeasurementResultSubjectTest {
  @Test
  fun `reach comparison passes`() {
    assertThat(PASSING_ACTUAL_RESULT)
      .reachValue()
      .isWithinPercent(COMPARISON_PERCENT)
      .of(EXPECTED_RESULT.reach.value)
  }

  @Test
  fun `reach comparison fails`() {
    val failure =
      expectFailureAbout(measurementResults()) { whenTesting ->
        whenTesting
          .that(PASSING_ACTUAL_RESULT.copy { reach = reach.copy { value = 1500 } })
          .reachValue()
          .isWithinPercent(COMPARISON_PERCENT)
          .of(EXPECTED_RESULT.reach.value)
      }
    assertThat(failure).factValue("expected to be within").isEqualTo("$COMPARISON_PERCENT%")
  }

  @Test
  fun `frequencyHistogram comparison passes`() {
    assertThat(PASSING_ACTUAL_RESULT)
      .frequencyDistribution()
      .isWithin(COMPARISON_TOLERANCE)
      .of(EXPECTED_RESULT.frequency.relativeFrequencyDistributionMap)
  }

  @Test
  fun `frequencyHistogram comparison fails`() {
    val failure =
      expectFailureAbout(measurementResults()) { whenTesting ->
        whenTesting
          .that(
            PASSING_ACTUAL_RESULT.copy {
              frequency = frequency.copy { relativeFrequencyDistribution[2] = 0.1234 }
            }
          )
          .frequencyDistribution()
          .isWithin(COMPARISON_TOLERANCE)
          .of(EXPECTED_RESULT.frequency.relativeFrequencyDistributionMap)
      }
    assertThat(failure).factValue("outside tolerance").isEqualTo("$COMPARISON_TOLERANCE")
  }

  companion object {
    private const val COMPARISON_PERCENT = 10.0
    private const val COMPARISON_TOLERANCE = 0.05

    private val EXPECTED_RESULT: Measurement.Result = result {
      reach = reach { value = 1296 }
      frequency = frequency {
        relativeFrequencyDistribution[1] = 0.3078817733990148
        relativeFrequencyDistribution[2] = 0.6042692939244664
        relativeFrequencyDistribution[3] = 0.031198686371100164
        relativeFrequencyDistribution[4] = 0.05008210180623974
        relativeFrequencyDistribution[5] = 0.004105090311986864
        relativeFrequencyDistribution[6] = 0.0024630541871921183
      }
    }

    private val PASSING_ACTUAL_RESULT: Measurement.Result = result {
      reach = reach { value = 1308 }
      frequency = frequency {
        relativeFrequencyDistribution[1] = 0.3058446757405925
        relativeFrequencyDistribution[2] = 0.6132906325060048
        relativeFrequencyDistribution[3] = 0.01120896717373899
        relativeFrequencyDistribution[4] = 0.036028823058446756
        relativeFrequencyDistribution[5] = 0.0040032025620496394
        relativeFrequencyDistribution[9] = 0.029623698959167333
      }
    }
  }
}
