// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import com.google.protobuf.Any as ProtoAny
import org.junit.Assert.assertThrows
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.common
import org.wfanet.measurement.api.v2alpha.populationSpec

/**
 * Covers how [HighOverlapExpectedReach] resolves a VID's demographic cell.
 *
 * `EdpAggregatorReportingIntegrationTest` compares per-cell metrics against this resolution, so a
 * cell assigned to the wrong stripe would move the expectation and the measurement apart and read
 * as a product failure. These cases use hand-built specs rather than the generated dataset, so they
 * pin the resolution itself rather than the generator's current output.
 */
@RunWith(JUnit4::class)
class HighOverlapExpectedReachTest {

  @Test
  fun `index resolves every VID in a stripe to that stripe's cell`() {
    val index = HighOverlapExpectedReach.DemographicIndex(stripedSpec(stripes = 4))

    // First, interior and last VID of stripe 0 all resolve to the same cell.
    assertThat(index.keyOf(1)).isEqualTo(keyOf(0))
    assertThat(index.keyOf(STRIPE_SIZE / 2)).isEqualTo(keyOf(0))
    assertThat(index.keyOf(STRIPE_SIZE)).isEqualTo(keyOf(0))
  }

  @Test
  fun `index resolves stripe boundaries to adjacent cells`() {
    val index = HighOverlapExpectedReach.DemographicIndex(stripedSpec(stripes = 4))

    // The VID either side of a stripe edge belongs to a different tuple.
    assertThat(index.keyOf(STRIPE_SIZE)).isEqualTo(keyOf(0))
    assertThat(index.keyOf(STRIPE_SIZE + 1)).isEqualTo(keyOf(1))
    assertThat(index.keyOf(2 * STRIPE_SIZE)).isEqualTo(keyOf(1))
    assertThat(index.keyOf(2 * STRIPE_SIZE + 1)).isEqualTo(keyOf(2))
  }

  @Test
  fun `index returns null outside every declared range`() {
    val index = HighOverlapExpectedReach.DemographicIndex(stripedSpec(stripes = 2))

    assertThat(index.keyOf(0)).isNull()
    assertThat(index.keyOf(2 * STRIPE_SIZE + 1)).isNull()
  }

  @Test
  fun `index collapses us_state so only gender and age_group distinguish cells`() {
    // Two stripes differing only by us_state must land in one cell: the report groups on gender
    // and age_group alone.
    val spec = populationSpec {
      subpopulations += subPopulationOf(1, STRIPE_SIZE, Common.UsState.CALIFORNIA)
      subpopulations += subPopulationOf(STRIPE_SIZE + 1, 2 * STRIPE_SIZE, Common.UsState.NEW_YORK)
    }
    val index = HighOverlapExpectedReach.DemographicIndex(spec)

    assertThat(index.keys).hasSize(1)
    assertThat(index.keyOf(1)).isEqualTo(index.keyOf(STRIPE_SIZE + 1))
  }

  @Test
  fun `population size per cell sums to the declared population`() {
    val stripes = 6
    val index = HighOverlapExpectedReach.DemographicIndex(stripedSpec(stripes))

    assertThat(index.populationSizeByKey.values.sum()).isEqualTo(stripes * STRIPE_SIZE)
  }

  @Test
  fun `population size per cell includes an unreached filler range`() {
    // The generated dataset parks every unreached VID in one trailing subpopulation, so one cell
    // is far larger than the rest. The expectation has to carry that asymmetry.
    val filler = 1_000_000L
    val spec = populationSpec {
      subpopulations += subPopulationOf(1, STRIPE_SIZE, Common.UsState.CALIFORNIA)
      subpopulations +=
        subPopulationOf(STRIPE_SIZE + 1, STRIPE_SIZE + filler, Common.UsState.CALIFORNIA)
    }
    val index = HighOverlapExpectedReach.DemographicIndex(spec)

    assertWithMessage("filler is folded into its cell")
      .that(index.populationSizeByKey.getValue(keyOf(0)))
      .isEqualTo(STRIPE_SIZE + filler)
  }

  @Test
  fun `index rejects a spec that does not start at VID 1`() {
    val spec = populationSpec {
      subpopulations += subPopulationOf(STRIPE_SIZE + 1, 2 * STRIPE_SIZE, Common.UsState.CALIFORNIA)
    }

    val exception =
      assertThrows(IllegalArgumentException::class.java) {
        HighOverlapExpectedReach.DemographicIndex(spec)
      }
    assertThat(exception).hasMessageThat().contains("expected to start at 1")
  }

  @Test
  fun `index rejects a gap between stripes`() {
    // An aligned stripe that skips a block would shift every later stripe out from under the
    // index arithmetic, silently mis-assigning demographics.
    val spec = populationSpec {
      subpopulations += subPopulationOf(1, STRIPE_SIZE, Common.UsState.CALIFORNIA)
      subpopulations +=
        subPopulationOf(2 * STRIPE_SIZE + 1, 3 * STRIPE_SIZE, Common.UsState.NEW_YORK)
    }
    val index = HighOverlapExpectedReach.DemographicIndex(spec)

    // The run stops at the gap, so VIDs past it fall back to a range scan rather than the index.
    assertThat(index.keyOf(STRIPE_SIZE)).isEqualTo(keyOf(0))
    assertThat(index.keyOf(2 * STRIPE_SIZE)).isNull()
    assertThat(index.keyOf(2 * STRIPE_SIZE + 1)).isEqualTo(keyOf(0))
  }

  /** A spec of [stripes] equal, aligned stripes cycling through the demographic tuples. */
  private fun stripedSpec(stripes: Int): PopulationSpec = populationSpec {
    for (i in 0 until stripes) {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = i * STRIPE_SIZE + 1
              endVidInclusive = (i + 1) * STRIPE_SIZE
            }
          attributes += ProtoAny.pack(TUPLES[i % TUPLES.size])
        }
    }
  }

  private fun subPopulationOf(start: Long, endInclusive: Long, state: Common.UsState) =
    PopulationSpecKt.subPopulation {
      vidRanges +=
        PopulationSpecKt.vidRange {
          startVid = start
          endVidInclusive = endInclusive
        }
      attributes +=
        ProtoAny.pack(
          common {
            gender = Common.Gender.MALE
            ageGroup = Common.AgeGroup.YEARS_18_TO_34
            usState = state
          }
        )
    }

  private fun keyOf(tupleIndex: Int): String =
    HighOverlapExpectedReach.demographicKey(TUPLES[tupleIndex % TUPLES.size])

  companion object {
    private const val STRIPE_SIZE = 10_000L

    /** Distinct `gender x age_group` tuples, which is what a cell key is built from. */
    private val TUPLES: List<Common> =
      listOf(Common.Gender.MALE, Common.Gender.FEMALE).flatMap { genderValue ->
        listOf(
            Common.AgeGroup.YEARS_18_TO_34,
            Common.AgeGroup.YEARS_35_TO_54,
            Common.AgeGroup.YEARS_55_PLUS,
          )
          .map { ageGroupValue ->
            common {
              gender = genderValue
              ageGroup = ageGroupValue
            }
          }
      }
  }
}
