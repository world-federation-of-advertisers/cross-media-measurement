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
import com.google.protobuf.TypeRegistry
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.VidRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
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
      ImpressionTestDataConfigs.resolveSpecPath(config.populationSpecResourcePath),
      PopulationSpec.getDefaultInstance(),
      TYPE_REGISTRY,
    )
  }

  private data class SeededRange(val edpName: String, val entityId: String, val range: VidRange)

  /** Every VID range in the generated specs, with the EDP and entity key it is seeded on. */
  private val seededRanges: List<SeededRange> by lazy {
    config.eventGroupsList.flatMap { eventGroup ->
      eventGroup.entityKeySpecsList.flatMap { entityKeySpec ->
        val spec =
          ImpressionTestDataConfigs.resolveSyntheticEventGroupSpec(
            entityKeySpec.dataSpecResourcePath
          )
        spec.dateSpecsList.flatMap { dateSpec ->
          dateSpec.frequencySpecsList.flatMap { frequencySpec ->
            frequencySpec.vidRangeSpecsList.map {
              SeededRange(eventGroup.edpName, entityKeySpec.entityId, it.vidRange)
            }
          }
        }
      }
    }
  }

  /** The EDPs seeded on each VID range, which is what defines a Venn region. */
  private val edpsByVidRangeStart: Map<Long, Set<String>> by lazy {
    seededRanges
      .groupBy { it.range.start }
      .mapValues { (_, seeded) -> seeded.map { it.edpName }.toSet() }
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
  fun `config declares the documented event group and entity key counts`() {
    assertWithMessage("event groups").that(config.eventGroupsList).hasSize(EVENT_GROUP_COUNT)

    val entityKeys = config.eventGroupsList.sumOf { it.entityKeySpecsCount }
    assertWithMessage("entity key specs").that(entityKeys).isEqualTo(ENTITY_KEY_COUNT)
  }

  @Test
  fun `segment VID ranges stay inside the reached range`() {
    for (seeded in seededRanges) {
      assertWithMessage("${seeded.entityId} on ${seeded.edpName}: range start")
        .that(seeded.range.start)
        .isAtLeast(1L)
      // Exclusive end, so the last valid value is one past the last reached VID.
      assertWithMessage("${seeded.entityId} on ${seeded.edpName}: range end")
        .that(seeded.range.endExclusive)
        .isAtMost(REACHED_VID_END + 1)
      assertWithMessage("${seeded.entityId} on ${seeded.edpName}: range is non-empty")
        .that(seeded.range.endExclusive)
        .isGreaterThan(seeded.range.start)
    }
  }

  companion object {
    private const val CONFIG_FILE = "high_overlap_impression_test_data_config.textproto"
    /** The population spec packs its demographics into `Any`, so parsing needs the descriptor. */
    private val TYPE_REGISTRY: TypeRegistry =
      TypeRegistry.newBuilder().add(Common.getDescriptor()).build()

    private const val TOTAL_POPULATION = 360_000_000L
    private const val REACHED_VID_END = 10_610_000L
    private const val STRIPE_SIZE = 10_000L
    private const val STRIPE_COUNT = (REACHED_VID_END / STRIPE_SIZE).toInt()

    /** `gender x age_group x us_state` over the values the dataset stripes through. */
    private const val DEMOGRAPHIC_TUPLES = 2 * 3 * 5

    /** Every non-empty subset of the four EDPs. */
    private const val VENN_REGIONS = 15
    private const val EDP_COUNT = 4

    /** One EventGroup per EDP per segment. */
    private const val EVENT_GROUP_COUNT = 33

    /** Each segment's entity keys, once per EDP the segment is seeded on. */
    private const val ENTITY_KEY_COUNT = 53
  }
}
