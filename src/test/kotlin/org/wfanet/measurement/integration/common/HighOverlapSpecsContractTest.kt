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
import com.google.protobuf.TypeRegistry
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.VidRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.common
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration

/**
 * Pins the shape of the high overlap dataset.
 *
 * `EdpAggregatorReportingIntegrationTest` derives its expected reach from these same specs, so it
 * cannot distinguish a fixture change from a system change: both sides move together. These
 * assertions are written against the design documented in
 * `docs/edpaggregator/high-overlap-data-shape.md`, so a fixture edit that drops a Venn region,
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
  fun `population spec covers every state age and gender cohort`() {
    val populationByDemographic: Map<Common, Long> =
      populationSpec.subpopulationsList
        .groupBy { it.attributesList.single().unpack(Common::class.java) }
        .mapValues { (_, subpopulations) ->
          subpopulations.sumOf { subpopulation ->
            subpopulation.vidRangesList.sumOf { it.endVidInclusive - it.startVid + 1 }
          }
        }
    val states =
      Common.UsState.values().filter {
        it != Common.UsState.UNRECOGNIZED &&
          it.number > 0 &&
          it != Common.UsState.DISTRICT_OF_COLUMBIA
      }
    assertThat(states).hasSize(50)
    assertThat(populationByDemographic).hasSize(2 * 3 * 50)
    for (state in states) {
      for (gender in listOf(Common.Gender.MALE, Common.Gender.FEMALE)) {
        for (age in
          listOf(
            Common.AgeGroup.YEARS_18_TO_34,
            Common.AgeGroup.YEARS_35_TO_54,
            Common.AgeGroup.YEARS_55_PLUS,
          )) {
          val demographics = common {
            usState = state
            this.gender = gender
            ageGroup = age
          }
          assertWithMessage("population for $state / $gender / $age")
            .that(populationByDemographic[demographics] ?: 0L)
            .isGreaterThan(0L)
        }
      }
    }
  }

  @Test
  fun `population VID ranges remain contiguous and ordered`() {
    var nextVid = 1L
    for (range in populationSpec.subpopulationsList.flatMap { it.vidRangesList }) {
      assertWithMessage("range starting at ${range.startVid}")
        .that(range.startVid)
        .isEqualTo(nextVid)
      assertThat(range.endVidInclusive).isAtLeast(range.startVid)
      nextVid = range.endVidInclusive + 1
    }
    assertThat(nextVid).isEqualTo(TOTAL_POPULATION + 1)
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
    assertWithMessage("reached stripes")
      .that(reached.sumOf { it.vidRangesCount })
      .isEqualTo(STRIPE_COUNT)
    val reachedDemographics =
      reached.map { it.attributesList.single().unpack(Common::class.java) }.toSet()
    assertThat(reachedDemographics).hasSize(2 * 3 * 50)
    assertThat(reachedDemographics.map { it.usState }.toSet()).hasSize(50)

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
  fun `every Venn region includes all state age and gender cohorts`() {
    val demographicsByStripe =
      populationSpec.subpopulationsList
        .flatMap { subpopulation ->
          val demographic = subpopulation.attributesList.single().unpack(Common::class.java)
          subpopulation.vidRangesList
            .filter { it.endVidInclusive <= REACHED_VID_END }
            .map { (it.startVid - 1) / STRIPE_SIZE to demographic }
        }
        .toMap()
    val demographicsByRegion = mutableMapOf<Set<String>, MutableSet<Common>>()
    for ((vidStart, edps) in edpsByVidRangeStart) {
      demographicsByRegion
        .getOrPut(edps) { mutableSetOf() }
        .add(demographicsByStripe.getValue((vidStart - 1) / STRIPE_SIZE))
    }
    assertThat(demographicsByRegion).hasSize(VENN_REGIONS)
    for ((edps, demographics) in demographicsByRegion) {
      assertWithMessage("demographic cohorts in region $edps").that(demographics).hasSize(300)
    }
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

  @Test
  fun `every reached stripe includes all device types for both media`() {
    for (eventGroup in config.eventGroupsList) {
      for (entity in eventGroup.entityKeySpecsList) {
        val ranges =
          ImpressionTestDataConfigs.resolveSyntheticEventGroupSpec(entity.dataSpecResourcePath)
            .dateSpecsList
            .flatMap { it.frequencySpecsList }
            .flatMap { it.vidRangeSpecsList }
        val devicesByStripe = ranges.groupBy { (it.vidRange.start - 1) / STRIPE_SIZE }
        for ((stripe, deviceRanges) in devicesByStripe) {
          val rangesByMedia =
            deviceRanges.groupBy { "video.completed_fraction" in it.nonPopulationFieldValuesMap }
          assertThat(rangesByMedia.keys).containsExactly(true, false)
          for (mediaRanges in rangesByMedia.values) {
            val devices =
              mediaRanges.map {
                it.nonPopulationFieldValuesMap.getValue("common.device_type").enumValue
              }
            assertWithMessage("devices on stripe $stripe for ${entity.entityId}")
              .that(devices)
              .containsExactly(
                Common.DeviceType.DESKTOP.number,
                Common.DeviceType.MOBILE.number,
                Common.DeviceType.TABLET.number,
              )
          }
          assertThat(
              deviceRanges
                .map { it.nonPopulationFieldValuesMap.getValue("common.signed_in").boolValue }
                .toSet()
            )
            .containsExactly(true, false)
          val ordered = deviceRanges.sortedBy { it.vidRange.start }
          for ((previous, next) in ordered.zipWithNext()) {
            assertThat(previous.vidRange.endExclusive).isEqualTo(next.vidRange.start)
          }
        }
      }
    }
  }

  @Test
  fun `generated impressions retain device geo demographics and media fields`() {
    val source =
      ImpressionTestDataConfigs.resolveSyntheticEventGroupSpec(
        config.eventGroupsList.first().entityKeySpecsList.single().dataSpecResourcePath
      )
    val firstDate = source.dateSpecsList.first()
    val firstFrequency = firstDate.frequencySpecsList.first()
    val samples =
      firstFrequency.vidRangeSpecsList.take(6).map { range ->
        range
          .toBuilder()
          .setVidRange(range.vidRange.toBuilder().setEndExclusive(range.vidRange.start + 1))
          .build()
      }
    val sampleSpec =
      source
        .toBuilder()
        .clearDateSpecs()
        .addDateSpecs(
          firstDate
            .toBuilder()
            .clearFrequencySpecs()
            .addFrequencySpecs(
              firstFrequency.toBuilder().clearVidRangeSpecs().addAllVidRangeSpecs(samples)
            )
        )
        .build()
    val events =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          sampleSpec,
        )
        .flatMap { it.labeledEvents }
        .toList()
    assertThat(events).hasSize(6)
    assertThat(events.map { it.message.common.deviceType }.toSet())
      .containsExactly(
        Common.DeviceType.DESKTOP,
        Common.DeviceType.MOBILE,
        Common.DeviceType.TABLET,
      )
    assertThat(events.map { it.message.common.mediaType }.toSet()).hasSize(2)
    assertThat(events.map { it.message.common.signedIn }.toSet()).containsExactly(true, false)
    for (event in events) {
      assertThat(event.message.common.usState).isEqualTo(Common.UsState.ALABAMA)
      assertThat(event.message.common.gender).isEqualTo(Common.Gender.MALE)
      assertThat(event.message.common.ageGroup).isEqualTo(Common.AgeGroup.YEARS_18_TO_34)
      assertThat(event.message.hasVideo() != event.message.hasDisplay()).isTrue()
    }
  }

  @Test
  fun `two blobs per EDP carry five thousand unregistered keys`() {
    val stressGroups = config.eventGroupsList.filter { it.additionalEntityKeyCount > 0 }
    assertThat(stressGroups).hasSize(8)
    assertThat(stressGroups.groupingBy { it.edpName }.eachCount().values)
      .containsExactly(2, 2, 2, 2)
    assertThat(config.eventGroupsList.flatMap { it.entityKeySpecsList }.map { it.entityType })
      .doesNotContain("ad-unit")
    for (group in stressGroups) {
      assertThat(group.additionalEntityKeyCount).isEqualTo(5_000)
      assertThat(group.additionalEntityKeyDatesList).hasSize(1)
      val stressDate = group.additionalEntityKeyDatesList.single()
      val ranges =
        group.entityKeySpecsList.flatMap { entity ->
          ImpressionTestDataConfigs.resolveSyntheticEventGroupSpec(entity.dataSpecResourcePath)
            .dateSpecsList
        }
      assertThat(
          ranges.any {
            stressDate.toLocalDate() >= it.dateRange.start.toLocalDate() &&
              stressDate.toLocalDate() < it.dateRange.endExclusive.toLocalDate()
          }
        )
        .isTrue()
    }
  }

  companion object {
    private const val CONFIG_FILE = "high_overlap_impression_test_data_config.textproto"
    /** The population spec packs its demographics into `Any`, so parsing needs the descriptor. */
    private val TYPE_REGISTRY: TypeRegistry =
      TypeRegistry.newBuilder().add(Common.getDescriptor()).build()

    private const val TOTAL_POPULATION = 360_000_000L
    private const val REACHED_VID_END = 10_610_000L
    private const val STRIPE_SIZE = 1_000L
    private const val STRIPE_COUNT = (REACHED_VID_END / STRIPE_SIZE).toInt()

    /** Every non-empty subset of the four EDPs. */
    private const val VENN_REGIONS = 15
    private const val EDP_COUNT = 4

    /** One EventGroup per EDP per segment. */
    private const val EVENT_GROUP_COUNT = 33

    /** Each segment's entity keys, once per EDP the segment is seeded on. */
    private const val ENTITY_KEY_COUNT = 53
  }
}
