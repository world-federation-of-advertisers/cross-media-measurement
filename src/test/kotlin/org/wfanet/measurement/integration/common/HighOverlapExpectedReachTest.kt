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
import kotlin.test.assertFailsWith
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.MediaType
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.common
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.config.reporting.MetricSpecConfig

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
  @get:Rule val temp = TemporaryFolder()

  @Test
  fun `index resolves every VID in a stripe to that stripe's cell`() {
    val index =
      HighOverlapExpectedReach.DemographicIndex(
        stripedSpec(stripes = 4),
        cellKey = HighOverlapExpectedReach::demographicKey,
      )

    // First, interior and last VID of stripe 0 all resolve to the same cell.
    assertThat(index.keyOf(1)).isEqualTo(keyOf(0))
    assertThat(index.keyOf(STRIPE_SIZE / 2)).isEqualTo(keyOf(0))
    assertThat(index.keyOf(STRIPE_SIZE)).isEqualTo(keyOf(0))
  }

  @Test
  fun `index resolves stripe boundaries to adjacent cells`() {
    val index =
      HighOverlapExpectedReach.DemographicIndex(
        stripedSpec(stripes = 4),
        cellKey = HighOverlapExpectedReach::demographicKey,
      )

    // The VID either side of a stripe edge belongs to a different tuple.
    assertThat(index.keyOf(STRIPE_SIZE)).isEqualTo(keyOf(0))
    assertThat(index.keyOf(STRIPE_SIZE + 1)).isEqualTo(keyOf(1))
    assertThat(index.keyOf(2 * STRIPE_SIZE)).isEqualTo(keyOf(1))
    assertThat(index.keyOf(2 * STRIPE_SIZE + 1)).isEqualTo(keyOf(2))
  }

  @Test
  fun `index returns null outside every declared range`() {
    val index =
      HighOverlapExpectedReach.DemographicIndex(
        stripedSpec(stripes = 2),
        cellKey = HighOverlapExpectedReach::demographicKey,
      )

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
    val index =
      HighOverlapExpectedReach.DemographicIndex(
        spec,
        cellKey = HighOverlapExpectedReach::demographicKey,
      )

    assertThat(index.keys).hasSize(1)
    assertThat(index.keyOf(1)).isEqualTo(index.keyOf(STRIPE_SIZE + 1))
  }

  @Test
  fun `population size per cell sums to the declared population`() {
    val stripes = 6
    val index =
      HighOverlapExpectedReach.DemographicIndex(
        stripedSpec(stripes),
        cellKey = HighOverlapExpectedReach::demographicKey,
      )

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
    val index =
      HighOverlapExpectedReach.DemographicIndex(
        spec,
        cellKey = HighOverlapExpectedReach::demographicKey,
      )

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
      assertFailsWith<IllegalArgumentException> {
        HighOverlapExpectedReach.DemographicIndex(
          spec,
          cellKey = HighOverlapExpectedReach::demographicKey,
        )
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
    val index =
      HighOverlapExpectedReach.DemographicIndex(
        spec,
        cellKey = HighOverlapExpectedReach::demographicKey,
      )

    // The run stops at the gap, so VIDs past it fall back to a range scan rather than the index.
    assertThat(index.keyOf(STRIPE_SIZE)).isEqualTo(keyOf(0))
    assertThat(index.keyOf(2 * STRIPE_SIZE)).isNull()
    assertThat(index.keyOf(2 * STRIPE_SIZE + 1)).isEqualTo(keyOf(0))
  }

  @Test
  fun `custom cell key keeps state and parental status distinct`() {
    val index = HighOverlapExpectedReach.DemographicIndex(samplingPopulation(), ::populationCell)
    assertThat(index.keyOf(1)).isEqualTo("CALIFORNIA/NOT_PARENT")
    assertThat(index.keyOf(11)).isEqualTo("NEW_YORK/PARENT")
    assertThat(index.keyOf(21)).isEqualTo("DISTRICT_OF_COLUMBIA/PARENT")
    assertThat(index.populationSizeByKey.values).containsExactly(10L, 10L, 10L)
  }

  @Test
  fun `device media intervals deduplicate overlapping ranges and EDP copies`() {
    val ranges = deviceRanges(intervalConfig(), null)
    assertMidpoint(ranges.getValue("DESKTOP/VIDEO").getValue("ami"), 10.0)
    assertMidpoint(ranges.getValue("DESKTOP/DISPLAY").getValue("ami"), 10.0)
    assertMidpoint(ranges.getValue("MOBILE/DISPLAY").getValue("ami"), 5.0)
  }

  @Test
  fun `MRC selects viewable display and leaves video and zero cells at zero`() {
    val ranges = deviceRanges(intervalConfig(), null)
    assertMidpoint(ranges.getValue("DESKTOP/DISPLAY").getValue("mrc"), 10.0)
    assertMidpoint(ranges.getValue("DESKTOP/VIDEO").getValue("mrc"), 0.0)
    assertMidpoint(ranges.getValue("MOBILE/DISPLAY").getValue("mrc"), 0.0)
    assertMidpoint(ranges.getValue("TV/DISPLAY").getValue("ami"), 0.0)
  }

  @Test
  fun `signed in filtering selects intervals without counting EDP copies twice`() {
    val config = intervalConfig()
    val signedIn = deviceRanges(config, true)
    val signedOut = deviceRanges(config, false)
    assertMidpoint(signedIn.getValue("DESKTOP/VIDEO").getValue("ami"), 10.0)
    assertMidpoint(signedIn.getValue("MOBILE/DISPLAY").getValue("ami"), 0.0)
    assertMidpoint(signedOut.getValue("DESKTOP/VIDEO").getValue("ami"), 0.0)
    assertMidpoint(signedOut.getValue("MOBILE/DISPLAY").getValue("ami"), 5.0)
  }

  @Test
  fun `interval expectations reject a partial source flight`() {
    assertFailsWith<IllegalArgumentException> {
      HighOverlapExpectedReach.computeWholeIntervalRangesByDeviceAndMedia(
        intervalConfig(),
        REFERENCE_IDS,
        START,
        START.plusDays(13),
        metricConfig(),
        signedIn = null,
      )
    }
  }

  @Test
  fun `sampled weekly cell has the same values as replaying all reached cells`() {
    val config = samplingConfig()
    val cells = setOf("CALIFORNIA/NOT_PARENT", "NEW_YORK/PARENT")
    val full = weeklyRanges(config, cells)
    for (cell in cells) {
      val sampled = weeklyRanges(config, setOf(cell))
      assertThat(sampled.keys).containsExactly(cell)
      assertThat(sampled.getValue(cell)).isEqualTo(full.getValue(cell))
    }
  }

  @Test
  fun `zero impression reference cell retains a positive population and zero centered noise ranges`() {
    val zero =
      weeklyRanges(samplingConfig(), setOf("DISTRICT_OF_COLUMBIA/PARENT"))
        .getValue("DISTRICT_OF_COLUMBIA/PARENT")
    assertThat(zero.keys).containsExactly("ami", "mrc", "custom-video")
    for (weeks in zero.values) {
      assertThat(weeks).hasSize(2)
      for (week in weeks) {
        assertThat(week.cumulative.populationSize).isEqualTo(10L)
        assertMidpoint(week.cumulative.reach, 0.0)
        assertMidpoint(week.nonCumulative.reach, 0.0)
        assertThat(week.componentNonCumulativeReach.keys).containsExactly("one", "two")
      }
    }
    val total =
      HighOverlapExpectedReach.computeWholeIntervalRangesByDemographic(
          samplingConfig(),
          REFERENCE_IDS,
          listOf("one", "two"),
          samplingPopulation(),
          START,
          START.plusWeeks(2),
          metricConfig(),
          2,
          populationKey = ::populationCell,
          selectedCells = setOf("DISTRICT_OF_COLUMBIA/PARENT"),
        )
        .getValue("DISTRICT_OF_COLUMBIA/PARENT")
    for (metric in total.values) {
      assertThat(metric.populationSize).isEqualTo(10L)
      assertMidpoint(metric.reach, 0.0)
      assertThat(metric.stackedIncrementalReach).hasSize(2)
      metric.stackedIncrementalReach.forEach { assertMidpoint(it, 0.0) }
    }
  }

  @Test
  fun `sampled expectations reject a cell absent from the population`() {
    assertFailsWith<IllegalArgumentException> { weeklyRanges(samplingConfig(), setOf("missing")) }
  }

  @Test
  fun `whole demographic metrics include total counts ratios and component reach`() {
    val totals =
      HighOverlapExpectedReach.computeWholeIntervalRangesByDemographic(
          samplingConfig(),
          REFERENCE_IDS,
          listOf("one", "two"),
          samplingPopulation(),
          START,
          START.plusWeeks(2),
          metricConfig(),
          2,
          populationKey = ::populationCell,
          selectedCells = setOf("CALIFORNIA/NOT_PARENT"),
        )
        .getValue("CALIFORNIA/NOT_PARENT")
        .getValue("ami")
    assertMidpoint(totals.reach, 10.0)
    assertMidpoint(checkNotNull(totals.impressions), 20.0)
    assertMidpoint(checkNotNull(totals.averageFrequency), 2.0)
    assertMidpoint(checkNotNull(totals.percentReach), 100.0)
    assertThat(totals.componentReach.keys).containsExactly("one", "two")
    totals.componentReach.values.forEach { assertMidpoint(it, 10.0) }
  }

  @Test
  fun `impressions are clipped per EDP rather than after merging the union`() {
    val config =
      sourceConfig(
        listOf(sourceRange(1, 11, Common.DeviceType.DESKTOP, MediaType.VIDEO, true, 1.0F)),
        frequency = 2,
        days = 7,
      )
    val params =
      metricConfig()
        .toBuilder()
        .apply { impressionCountParamsBuilder.maximumFrequencyPerUser = 1 }
        .build()
    val total =
      HighOverlapExpectedReach.computeWholeIntervalRangesByDemographic(
          config,
          REFERENCE_IDS,
          listOf("one", "two"),
          samplingPopulation(),
          START,
          START.plusWeeks(2),
          params,
          2,
          populationKey = ::populationCell,
          selectedCells = setOf("CALIFORNIA/NOT_PARENT"),
        )
        .getValue("CALIFORNIA/NOT_PARENT")
        .getValue("ami")
    assertMidpoint(checkNotNull(total.impressions), 20.0)
    val weeks =
      HighOverlapExpectedReach.computeWeeklyRangesByDemographic(
          config,
          REFERENCE_IDS,
          samplingPopulation(),
          START,
          2,
          params,
          2,
          populationKey = ::populationCell,
          selectedCells = setOf("CALIFORNIA/NOT_PARENT"),
        )
        .getValue("CALIFORNIA/NOT_PARENT")
        .getValue("ami")
    assertMidpoint(weeks[0].nonCumulativeImpressions, 20.0)
    assertMidpoint(checkNotNull(weeks[1].cumulative.impressions), 20.0)
    assertMidpoint(weeks[1].nonCumulativeImpressions, 0.0)
  }

  @Test
  fun `device weekly metrics follow the source flight including an unreached later week`() {
    val config =
      sourceConfig(
        listOf(sourceRange(1, 6, Common.DeviceType.DESKTOP, MediaType.VIDEO, true, 1.0F)),
        days = 7,
        frequency = 1,
      )
    val series =
      HighOverlapExpectedReach.computeWeeklyRangesByDeviceAndMedia(
        config,
        REFERENCE_IDS,
        samplingPopulation(),
        START,
        2,
        metricConfig(),
        2,
        setOf("DESKTOP/VIDEO", "TV/DISPLAY"),
      )
    val weeks = series.getValue("DESKTOP/VIDEO").getValue("ami")
    for (week in weeks) {
      assertMidpoint(week.cumulative.reach, 5.0)
      assertMidpoint(checkNotNull(week.cumulative.impressions), 10.0)
      assertMidpoint(checkNotNull(week.cumulative.averageFrequency), 2.0)
      week.cumulative.kPlusReach.forEach { assertMidpoint(it, 5.0) }
      week.cumulative.componentReach.values.forEach { assertMidpoint(it, 5.0) }
    }
    assertMidpoint(weeks[0].nonCumulative.reach, 5.0)
    assertMidpoint(weeks[0].nonCumulativeImpressions, 10.0)
    assertMidpoint(weeks[0].nonCumulativeAverageFrequency, 2.0)
    weeks[0].nonCumulative.kPlusReach.forEach { assertMidpoint(it, 5.0) }
    weeks[0].componentNonCumulativeReach.values.forEach { assertMidpoint(it, 5.0) }
    assertMidpoint(weeks[1].nonCumulative.reach, 0.0)
    assertMidpoint(weeks[1].nonCumulativeImpressions, 0.0)
    weeks[1].nonCumulative.kPlusReach.forEach { assertMidpoint(it, 0.0) }
    weeks[1].componentNonCumulativeReach.values.forEach { assertMidpoint(it, 0.0) }
    for (filters in
      listOf(
        series.getValue("TV/DISPLAY"),
        series.getValue("DESKTOP/VIDEO").filterKeys { it == "mrc" },
      )) {
      for (zeroWeeks in filters.values) for (week in zeroWeeks) {
        assertMidpoint(week.cumulative.reach, 0.0)
        assertMidpoint(checkNotNull(week.cumulative.impressions), 0.0)
      }
    }
  }

  @Test
  fun `device total metrics apply signed in and MRC filters before counting frequencies`() {
    val config = samplingConfig()
    fun totals(signedIn: Boolean) =
      HighOverlapExpectedReach.computeWholeIntervalMetricsByDeviceAndMedia(
        config,
        REFERENCE_IDS,
        listOf("one", "two"),
        samplingPopulation(),
        START,
        START.plusWeeks(2),
        metricConfig(),
        2,
        setOf("DESKTOP/VIDEO", "MOBILE/DISPLAY"),
        signedIn,
      )
    val signedIn = totals(true)
    val signedOut = totals(false)
    val populated =
      listOf(
        signedIn.getValue("DESKTOP/VIDEO").getValue("ami"),
        signedOut.getValue("MOBILE/DISPLAY").getValue("mrc"),
      )
    for (metric in populated) {
      assertMidpoint(metric.reach, 5.0)
      metric.kPlusReach.forEach { assertMidpoint(it, 5.0) }
      assertMidpoint(checkNotNull(metric.impressions), 10.0)
      assertMidpoint(checkNotNull(metric.averageFrequency), 2.0)
      metric.componentReach.values.forEach { assertMidpoint(it, 5.0) }
    }
    val zeros =
      listOf(
        signedIn.getValue("DESKTOP/VIDEO").getValue("mrc"),
        signedOut.getValue("DESKTOP/VIDEO").getValue("ami"),
        signedIn.getValue("MOBILE/DISPLAY").getValue("ami"),
      )
    for (metric in zeros) {
      assertMidpoint(metric.reach, 0.0)
      metric.kPlusReach.forEach { assertMidpoint(it, 0.0) }
      assertMidpoint(checkNotNull(metric.impressions), 0.0)
      metric.componentReach.values.forEach { assertMidpoint(it, 0.0) }
    }
  }

  private fun assertMidpoint(range: ClosedFloatingPointRange<Double>, expected: Double) {
    assertThat((range.start + range.endInclusive) / 2).isWithin(0.000001).of(expected)
  }

  private fun populationCell(common: Common) =
    "${common.usState.name}/${common.parentalStatus.name}"

  private fun samplingPopulation() = populationSpec {
    for ((index, pair) in
      listOf(
          Common.UsState.CALIFORNIA to Common.ParentalStatus.NOT_PARENT,
          Common.UsState.NEW_YORK to Common.ParentalStatus.PARENT,
          Common.UsState.DISTRICT_OF_COLUMBIA to Common.ParentalStatus.PARENT,
        )
        .withIndex()) {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          attributes +=
            ProtoAny.pack(
              common {
                gender = Common.Gender.MALE
                ageGroup = Common.AgeGroup.YEARS_18_TO_34
                usState = pair.first
                parentalStatus = pair.second
              }
            )
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = index * 10L + 1
              endVidInclusive = (index + 1) * 10L
            }
        }
    }
  }

  private fun weeklyRanges(config: ImpressionTestDataConfig, cells: Set<String>) =
    HighOverlapExpectedReach.computeWeeklyRangesByDemographic(
      config,
      REFERENCE_IDS,
      samplingPopulation(),
      START,
      2,
      metricConfig(),
      2,
      populationKey = ::populationCell,
      selectedCells = cells,
    )

  private fun deviceRanges(config: ImpressionTestDataConfig, signedIn: Boolean?) =
    HighOverlapExpectedReach.computeWholeIntervalRangesByDeviceAndMedia(
      config,
      REFERENCE_IDS,
      START,
      START.plusWeeks(2),
      metricConfig(),
      signedIn,
    )

  private fun metricConfig(): MetricSpecConfig {
    val privacy =
      MetricSpecConfig.DifferentialPrivacyParams.newBuilder().setEpsilon(1.0).setDelta(0.000001)
    val sampling =
      MetricSpecConfig.VidSamplingInterval.newBuilder()
        .setFixedStart(
          MetricSpecConfig.VidSamplingInterval.FixedStart.newBuilder().setStart(0.0F).setWidth(1.0F)
        )
    return MetricSpecConfig.newBuilder()
      .setReachParams(
        MetricSpecConfig.ReachParams.newBuilder()
          .setSingleDataProviderParams(
            MetricSpecConfig.SamplingAndPrivacyParams.newBuilder()
              .setPrivacyParams(privacy)
              .setVidSamplingInterval(sampling)
          )
      )
      .setReachAndFrequencyParams(
        MetricSpecConfig.ReachAndFrequencyParams.newBuilder()
          .setMultipleDataProviderParams(
            MetricSpecConfig.ReachAndFrequencySamplingAndPrivacyParams.newBuilder()
              .setReachPrivacyParams(privacy)
              .setFrequencyPrivacyParams(privacy)
              .setVidSamplingInterval(sampling)
          )
      )
      .setImpressionCountParams(
        MetricSpecConfig.ImpressionCountParams.newBuilder()
          .setParams(
            MetricSpecConfig.SamplingAndPrivacyParams.newBuilder()
              .setPrivacyParams(privacy)
              .setVidSamplingInterval(sampling)
          )
          .setMaximumFrequencyPerUser(10)
      )
      .build()
  }

  private fun intervalConfig() =
    sourceConfig(
      listOf(
        sourceRange(1, 11, Common.DeviceType.DESKTOP, MediaType.VIDEO, true, 1.0F),
        sourceRange(6, 16, Common.DeviceType.DESKTOP, MediaType.DISPLAY, true, 1.0F),
        sourceRange(8, 12, Common.DeviceType.DESKTOP, MediaType.DISPLAY, true, 1.0F),
        sourceRange(16, 21, Common.DeviceType.MOBILE, MediaType.DISPLAY, false, 0.0F),
      ),
      frequency = 1,
      days = 14,
    )

  private fun samplingConfig() =
    sourceConfig(
      listOf(
        sourceRange(1, 6, Common.DeviceType.DESKTOP, MediaType.VIDEO, true, 1.0F),
        sourceRange(6, 11, Common.DeviceType.DESKTOP, MediaType.DISPLAY, true, 1.0F),
        sourceRange(11, 16, Common.DeviceType.MOBILE, MediaType.VIDEO, false, 1.0F),
        sourceRange(16, 21, Common.DeviceType.MOBILE, MediaType.DISPLAY, false, 1.0F),
      ),
      frequency = 1,
      days = 14,
    )

  private fun sourceRange(
    start: Long,
    end: Long,
    device: Common.DeviceType,
    media: MediaType,
    signedIn: Boolean,
    engagement: Float,
  ): String =
    """
    vid_range_specs {
      vid_range { start: $start end_exclusive: $end }
      non_population_field_values { key: "common.device_type" value { enum_value: ${device.number} } }
      non_population_field_values { key: "common.media_type" value { enum_value: ${media.number} } }
      non_population_field_values { key: "common.signed_in" value { bool_value: $signedIn } }
      non_population_field_values { key: "${if (media == MediaType.VIDEO) "video.completed_fraction" else "display.viewable_fraction"}" value { float_value: $engagement } }
    }
  """
      .trimIndent()

  private fun sourceConfig(
    ranges: List<String>,
    frequency: Int,
    days: Int,
  ): ImpressionTestDataConfig {
    val source = temp.newFile()
    source.writeText(
      """
      date_specs {
        date_range { start { year: 2026 month: 4 day: 1 } end_exclusive { year: 2026 month: 4 day: ${days + 1} } }
        frequency_specs { frequency: $frequency ${ranges.joinToString("\n")} }
      }
    """
        .trimIndent()
    )
    return ImpressionTestDataConfig.newBuilder()
      .apply {
        for (edp in listOf("one", "two")) {
          addEventGroups(
            ImpressionTestDataConfig.SyntheticEventGroup.newBuilder()
              .setEdpName(edp)
              .addEntityKeySpecs(
                ImpressionTestDataConfig.EntityKeySpec.newBuilder()
                  .setEntityType("campaign")
                  .setEntityId(edp)
                  .setDataSpecResourcePath(source.absolutePath)
              )
          )
        }
      }
      .build()
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
    private val START = java.time.LocalDate.of(2026, 4, 1)
    private val REFERENCE_IDS = setOf("campaign-one", "campaign-two")

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
