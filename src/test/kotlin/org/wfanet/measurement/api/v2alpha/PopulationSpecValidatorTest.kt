/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.api.v2alpha

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange

@RunWith(JUnit4::class)
class PopulationSpecValidatorTest {
  @Test
  fun `vidRange invalid with start greater than end`() {
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 2
          endVidInclusive = 1
        }
      }
    }
    val result = PopulationSpecValidator.validateVidRangesList(testPopulationSpec)
    val exception = result.exceptionOrNull() as PopulationSpecValidationException
    assertThat(exception).isNotNull()
    assertThat(exception.errors.size).isEqualTo(1)
    assertThat(exception.errors[0].toString())
      .isEqualTo(
        "The endVidInclusive of the range at 'SubpopulationIndex: 0 VidRangeIndex: 0' " +
          "must be greater than or equal to the startVid."
      )
  }

  @Test
  fun `vidRange invalid with start non-positive`() {
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 0
          endVidInclusive = 1
        }
      }
    }
    val result = PopulationSpecValidator.validateVidRangesList(testPopulationSpec)
    val exception = result.exceptionOrNull() as PopulationSpecValidationException
    assertThat(exception).isNotNull()
    assertThat(exception.errors.size).isEqualTo(1)
    assertThat(exception.errors[0].toString())
      .isEqualTo(
        "The startVid of the range at 'SubpopulationIndex: 0 VidRangeIndex: 0' " +
          "must be greater than zero."
      )
  }

  @Test
  fun `two vidRanges across subpopulation non-disjoint`() {
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 2
        }
      }
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 2
          endVidInclusive = 3
        }
      }
    }
    val result = PopulationSpecValidator.validateVidRangesList(testPopulationSpec)
    val exception = result.exceptionOrNull() as PopulationSpecValidationException
    assertThat(exception).isNotNull()
    assertThat(exception.errors.size).isEqualTo(1)
    assertThat(exception.errors[0].toString())
      .isEqualTo(
        "The ranges at 'SubpopulationIndex: 0 VidRangeIndex: 0' and 'SubpopulationIndex: 1 " +
          "VidRangeIndex: 0' must be disjoint."
      )
  }

  @Test
  fun `two error accumulation single range invalid and vidRanges are non-disjoint`() {
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        // This range is invalid.
        vidRanges += vidRange {
          startVid = 0
          endVidInclusive = 1
        }
        // This one is non-disjoint with the one right below
        vidRanges += vidRange {
          startVid = 2
          endVidInclusive = 3
        }
      }
      subpopulations += subPopulation {
        // This one is non-disjoint with the one right above
        vidRanges += vidRange {
          startVid = 3
          endVidInclusive = 4
        }
        // This one is valid and disjoint!
        vidRanges += vidRange {
          startVid = 300
          endVidInclusive = 400
        }
      }
    }
    val result = PopulationSpecValidator.validateVidRangesList(testPopulationSpec)
    val exception = result.exceptionOrNull() as PopulationSpecValidationException
    assertThat(exception).isNotNull()
    assertThat(exception.errors.size).isEqualTo(2)
    assertThat(exception.errors[0].toString())
      .isEqualTo(
        "The startVid of the range at 'SubpopulationIndex: 0 VidRangeIndex: 0' " +
          "must be greater than zero."
      )
    assertThat(exception.errors[1].toString())
      .isEqualTo(
        "The ranges at 'SubpopulationIndex: 0 VidRangeIndex: 1' and 'SubpopulationIndex: " +
          "1 VidRangeIndex: 0' must be disjoint."
      )
  }

 @Test
  fun `valid vidRange is valid`() {
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 1
        }
        vidRanges += vidRange {
          startVid = 2
          endVidInclusive = 3
        }
      }
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 4
          endVidInclusive = 5
        }
      }
    }
    assertThat(PopulationSpecValidator.validateVidRangesList(testPopulationSpec).getOrNull()).isTrue()
  }
}
