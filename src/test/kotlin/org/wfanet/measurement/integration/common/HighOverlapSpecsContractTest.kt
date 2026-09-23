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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.common.parseTextProto

/**
 * Pins the shape of the generated high overlap dataset.
 *
 * `EdpAggregatorReportingIntegrationTest` derives its expected reach from these same specs, so it
 * cannot distinguish a generator change from a system change: both sides move together. These
 * assertions are written against the design documented in
 * `docs/edpaggregator/high-overlap-data-shape.md`, so a generator edit that drops a Venn region,
 * breaks the demographic striping or resizes the population fails here instead of silently
 * redefining what the integration test considers correct.
 */
@RunWith(JUnit4::class)
class HighOverlapSpecsContractTest {

  private val config: ImpressionTestDataConfig by lazy {
    parseTextProto(
      ImpressionTestDataConfigs.resolveSpecPath(CONFIG_FILE),
      ImpressionTestDataConfig.getDefaultInstance(),
    )
  }

  private val populationSpec: PopulationSpec by lazy {
    parseTextProto(
      ImpressionTestDataConfigs.resolveSpecPath(POPULATION_SPEC_FILE),
      PopulationSpec.getDefaultInstance(),
    )
  }

  /** VID range starts mapped to the EDPs seeded on them, which is what defines a Venn region. */
  private val edpsByVidRangeStart: Map<Long, Set<String>> by lazy {
    val byStart = mutableMapOf<Long, MutableSet<String>>()
    for (eventGroup in config.eventGroupsList) {
      for (entityKeySpec in eventGroup.entityKeySpecsList) {
        val spec =
          ImpressionTestDataConfigs.resolveSyntheticEventGroupSpec(
            entityKeySpec.dataSpecResourcePath
          )
        for (dateSpec in spec.dateSpecsList) {
          for (frequencySpec in dateSpec.frequencySpecsList) {
            for (vidRangeSpec in frequencySpec.vidRangeSpecsList) {
              byStart
                .getOrPut(vidRangeSpec.vidRange.start) { mutableSetOf() }
                .add(eventGroup.edpName)
            }
          }
        }
      }
    }
    byStart
  }

  @Test
  fun `population spec declares the documented size`() {
    val declared =
      populationSpec.subpopulationsList.sumOf { subpopulation ->
        subpopulation.vidRangesList.sumOf { it.endVidInclusive - it.startVid + 1 }
      }
    assertThat(declared).isEqualTo(TOTAL_POPULATION)
  }

  @Test
  fun `population spec cycles through every demographic combination`() {
    val tuples: Set<ProtoAny> =
      populationSpec.subpopulationsList.flatMap { it.attributesList }.toSet()
    assertWithMessage("distinct demographic tuples").that(tuples).hasSize(DEMOGRAPHIC_TUPLES)
  }

  @Test
  fun `reached subpopulations are uniform aligned stripes`() {
    // Striping keeps demographics orthogonal to the Venn structure. It is also load-bearing:
    // SyntheticDataGeneration resolves a range's demographics by finding the one subpopulation
    // wholly containing it, so a range spanning two stripes fails outright.
    val reached =
      populationSpec.subpopulationsList.filter { subpopulation ->
        subpopulation.vidRangesList.all { it.endVidInclusive <= REACHED_VID_END }
      }
    assertWithMessage("reached subpopulations").that(reached).hasSize(STRIPE_COUNT)

    for (subpopulation in reached) {
      for (range in subpopulation.vidRangesList) {
        assertWithMessage("stripe ${range.startVid}..${range.endVidInclusive}")
          .that(range.endVidInclusive - range.startVid + 1)
          .isEqualTo(STRIPE_SIZE)
        assertWithMessage("stripe ${range.startVid} alignment")
          .that((range.startVid - 1) % STRIPE_SIZE)
          .isEqualTo(0)
      }
    }
  }

  @Test
  fun `config realizes every non-empty Venn region over the four EDPs`() {
    val regions: Set<Set<String>> = edpsByVidRangeStart.values.toSet()
    assertWithMessage("distinct EDP combinations: $regions").that(regions).hasSize(VENN_REGIONS)

    val edps: Set<String> = regions.flatten().toSet()
    assertWithMessage("EDPs: $edps").that(edps).hasSize(EDP_COUNT)
  }

  @Test
  fun `config declares the documented entity key count`() {
    val entityKeys = config.eventGroupsList.sumOf { it.entityKeySpecsCount }
    assertThat(entityKeys).isEqualTo(ENTITY_KEY_COUNT)
  }

  @Test
  fun `segment VID ranges stay inside the reached range`() {
    for ((start, edps) in edpsByVidRangeStart) {
      assertWithMessage("range starting $start on $edps").that(start).isAtLeast(1L)
      assertWithMessage("range starting $start on $edps").that(start).isAtMost(REACHED_VID_END)
    }
  }

  companion object {
    private const val CONFIG_FILE = "high_overlap_impression_test_data_config.textproto"
    private const val POPULATION_SPEC_FILE = "high_overlap_population_spec.textproto"

    private const val TOTAL_POPULATION = 360_000_000L
    private const val REACHED_VID_END = 10_610_000L
    private const val STRIPE_SIZE = 10_000L
    private const val STRIPE_COUNT = (REACHED_VID_END / STRIPE_SIZE).toInt()

    /** `gender x age_group x us_state` over the values the dataset stripes through. */
    private const val DEMOGRAPHIC_TUPLES = 2 * 3 * 5

    /** Every non-empty subset of the four EDPs. */
    private const val VENN_REGIONS = 15
    private const val EDP_COUNT = 4

    /** Entity keys across every segment, one EventGroup each. */
    private const val ENTITY_KEY_COUNT = 26
  }
}
