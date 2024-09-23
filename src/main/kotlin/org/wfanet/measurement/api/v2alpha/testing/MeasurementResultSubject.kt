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
import com.google.common.truth.Truth.assertAbout
import com.google.common.truth.extensions.proto.ProtoSubject
import org.wfanet.measurement.api.v2alpha.Measurement

class MeasurementResultSubject
private constructor(failureMetadata: FailureMetadata, private val actual: Measurement.Result?) :
  ProtoSubject(failureMetadata, actual) {

  fun reachValue(): FuzzyLongSubject {
    return check("reach.value").about(FuzzyLongSubject.fuzzyLongs()).that(actual?.reach?.value)
  }

  fun frequencyDistribution(): FrequencyDistributionSubject {
    return check("frequency.relativeFrequencyDistribution")
      .about(FrequencyDistributionSubject.frequencyDistributions())
      .that(actual?.frequency?.relativeFrequencyDistributionMap)
  }

  fun impressionValue(): FuzzyLongSubject {
    return check("impression.value")
      .about(FuzzyLongSubject.fuzzyLongs())
      .that(actual?.impression?.value)
  }

  companion object {
    fun measurementResults() =
      Factory<MeasurementResultSubject, Measurement.Result> { metadata, actual ->
        MeasurementResultSubject(metadata, actual)
      }

    fun assertThat(subject: Measurement.Result): MeasurementResultSubject =
      assertAbout(measurementResults()).that(subject)
  }
}
