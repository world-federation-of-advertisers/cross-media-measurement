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

import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import kotlin.math.ln
import kotlin.math.sqrt
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration
import org.wfanet.measurement.loadtest.reporting.ReportingUserSimulator

/**
 * Expected reach for the high overlap media-type and impression-qualification-filter report,
 * derived from the synthetic specs rather than from the impressions the EDP Aggregator reads.
 *
 * Reach is the number of distinct VIDs matching a filter over the reporting interval, unscaled: the
 * measured value is already scaled back up from the VID sampling interval, which affects only the
 * tolerance.
 */
object HighOverlapExpectedReach {

  /** Acceptable range for every metric the report requests for one line item. */
  data class ExpectedMetrics(
    val reach: ClosedFloatingPointRange<Double>,
    /** Indexed by frequency - 1, so element `i` is the reach at frequency `i + 1` or more. */
    val kPlusReach: List<ClosedFloatingPointRange<Double>>,
    /** VIDs the population spec declares for this cell, which the report returns unnoised. */
    val populationSize: Long = 0L,
    /**
     * Per-component contribution in `reporting_unit.components` order, each the increment its
     * component adds to the union of the components before it.
     */
    val stackedIncrementalReach: List<ClosedFloatingPointRange<Double>> = emptyList(),
  )

  /** Expected metrics for one week of a weekly series. */
  data class ExpectedWeeklyMetrics(
    val cumulative: ExpectedMetrics,
    val nonCumulative: ExpectedMetrics,
    val cumulativePercentReach: ClosedFloatingPointRange<Double>,
    val nonCumulativeImpressions: ClosedFloatingPointRange<Double>,
    val nonCumulativeAverageFrequency: ClosedFloatingPointRange<Double>,
    /** Keyed by EDP display name. */
    val componentNonCumulativeReach: Map<String, ClosedFloatingPointRange<Double>>,
  )

  /** The demographic cell [common] belongs to, matching the report's `gender/ageGroup` grouping. */
  fun demographicKey(common: Common): String = "${common.gender.name}/${common.ageGroup.name}"

  /**
   * Resolves a VID's demographic cell from the population spec.
   *
   * The reached VID space is a run of equal, aligned stripes, so the stripe holding a VID is an
   * index rather than a search. The spec is the same file the EDP Aggregator labels against, so
   * this and the measured results agree by construction.
   */
  class DemographicIndex(populationSpec: PopulationSpec) {
    private class Stripe(val startVid: Long, val endVidInclusive: Long, val key: String)

    private val stripes: List<Stripe> =
      populationSpec.subpopulationsList
        .flatMap { subpopulation ->
          val key = demographicKey(subpopulation.attributesList.single().unpack(Common::class.java))
          subpopulation.vidRangesList.map { Stripe(it.startVid, it.endVidInclusive, key) }
        }
        .sortedBy { it.startVid }

    /** Size of the leading equal-and-contiguous run, which every reached VID falls in. */
    private val stripeSize: Long =
      stripes.firstOrNull()?.let { it.endVidInclusive - it.startVid + 1 }
        ?: error("Population spec declares no subpopulations")

    /**
     * Keys of that run, indexed by stripe. Beyond it the spec is a single unreached filler.
     *
     * Each stripe must start exactly where the previous one ended, so a gap cannot shift later
     * stripes out from under the index arithmetic in [keyOf].
     */
    private val keysByStripe: List<String> =
      stripes
        .withIndex()
        .takeWhile { (index, stripe) ->
          stripe.startVid == index * stripeSize + 1 &&
            stripe.endVidInclusive - stripe.startVid + 1 == stripeSize
        }
        .map { it.value.key }

    private val uniformEndVidInclusive: Long = keysByStripe.size * stripeSize

    init {
      // Guards the index arithmetic: a generator that stops striping uniformly, or leaves a gap,
      // would otherwise silently mis-assign demographics instead of failing here.
      require(keysByStripe.isNotEmpty()) {
        "Population spec does not begin with a contiguous run of equal stripes; first range is " +
          "${stripes.first().startVid}..${stripes.first().endVidInclusive}, expected to start at 1"
      }
    }

    /** Every cell the spec declares, so a cell nobody reached is still a known key. */
    val keys: Set<String> = stripes.map { it.key }.toSet()

    /** VIDs the spec declares per cell, including the unreached filler. */
    val populationSizeByKey: Map<String, Long> =
      stripes
        .groupBy { it.key }
        .mapValues { (_, striped) -> striped.sumOf { it.endVidInclusive - it.startVid + 1 } }

    /** Returns the cell holding [vid], or null when the spec declares no range for it. */
    fun keyOf(vid: Long): String? {
      if (vid in 1..uniformEndVidInclusive) {
        return keysByStripe[((vid - 1) / stripeSize).toInt()]
      }
      return stripes.firstOrNull { vid >= it.startVid && vid <= it.endVidInclusive }?.key
    }
  }

  /**
   * Returns the acceptable metric ranges per impression qualification filter label for each of the
   * two result groups.
   *
   * @param config high overlap config restricted to the provisioned EDPs
   * @param singleEdpName the EDP the single-EDP result group reports on
   * @param populationSpec the high overlap population spec
   * @param reportStart first event date, inclusive
   * @param reportEnd end of the reporting interval, exclusive
   * @param metricSpecConfig the deployed metric spec config, for the noise parameters
   * @param maxFrequency highest frequency the report requests K+ reach for
   */
  fun computeRangesByGroupAndFilter(
    config: ImpressionTestDataConfig,
    eventGroupReferenceIds: Set<String>,
    singleEdpName: String,
    populationSpec: PopulationSpec,
    reportStart: LocalDate,
    reportEnd: LocalDate,
    metricSpecConfig: MetricSpecConfig,
    maxFrequency: Int,
  ): Map<String, Map<String, ExpectedMetrics>> {
    val frequenciesByEdpAndFilter: Map<String, Map<String, Map<Long, Int>>> =
      frequenciesByEdpAndFilter(
        config,
        eventGroupReferenceIds,
        populationSpec,
        reportStart,
        reportEnd,
      )

    val singleEdpFrequencies: Map<String, Map<Long, Int>> =
      frequenciesByEdpAndFilter[singleEdpName]
        ?: error("No EventGroups for $singleEdpName among $eventGroupReferenceIds")
    // A VID reached through more than one EDP is one person with the impressions of both.
    val allEdpFrequencies: Map<String, Map<Long, Int>> =
      FILTER_PREDICATES.keys.associateWith { label ->
        val merged = mutableMapOf<Long, Int>()
        for (byFilter in frequenciesByEdpAndFilter.values) {
          for ((vid, frequency) in byFilter.getValue(label)) {
            merged[vid] = (merged[vid] ?: 0) + frequency
          }
        }
        merged
      }

    val reachAndFrequencyParams = metricSpecConfig.reachAndFrequencyParams

    return mapOf(
      ReportingUserSimulator.SINGLE_EDP_GROUP_TITLE to
        singleEdpFrequencies.mapValues { (_, frequencies) ->
          expectedMetrics(
            frequencies,
            reachAndFrequencyParams.singleDataProviderParams,
            maxFrequency,
          )
        },
      ReportingUserSimulator.CROSS_PUB_GROUP_TITLE to
        allEdpFrequencies.mapValues { (_, frequencies) ->
          expectedMetrics(
            frequencies,
            reachAndFrequencyParams.multipleDataProviderParams,
            maxFrequency,
          )
        },
    )
  }

  /**
   * Returns the acceptable ranges for the whole-interval result group, per demographic cell and
   * impression qualification filter.
   *
   * [componentEdpNames] must be the reporting unit's components in request order: the stacked
   * incremental reach of component `i` is what it adds to the union of those before it, so a
   * different order is a different expectation.
   */
  fun computeWholeIntervalRangesByDemographic(
    config: ImpressionTestDataConfig,
    eventGroupReferenceIds: Set<String>,
    componentEdpNames: List<String>,
    populationSpec: PopulationSpec,
    reportStart: LocalDate,
    reportEnd: LocalDate,
    metricSpecConfig: MetricSpecConfig,
    maxFrequency: Int,
  ): Map<String, Map<String, ExpectedMetrics>> {
    val demographics = DemographicIndex(populationSpec)
    val frequenciesByEdpAndFilter =
      frequenciesByEdpAndFilter(
        config,
        eventGroupReferenceIds,
        populationSpec,
        reportStart,
        reportEnd,
      )
    for (edpName in componentEdpNames) {
      require(frequenciesByEdpAndFilter.containsKey(edpName)) {
        "No EventGroups for component $edpName among $eventGroupReferenceIds"
      }
    }
    val params = metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
    val stackedTolerance =
      STACKED_NOISE_MULTIPLIER * tolerance(params.reachPrivacyParams, params.vidSamplingInterval)

    val byCell = mutableMapOf<String, MutableMap<String, ExpectedMetrics>>()
    for (label in FILTER_PREDICATES.keys) {
      // Union across EDPs, bucketed by cell: a VID reached through more than one EDP is one
      // person with the impressions of both.
      val unionByCell = mutableMapOf<String, MutableMap<Long, Int>>()
      for (byFilter in frequenciesByEdpAndFilter.values) {
        for ((vid, frequency) in byFilter.getValue(label)) {
          val key = demographics.keyOf(vid) ?: continue
          val frequencies = unionByCell.getOrPut(key) { mutableMapOf() }
          frequencies[vid] = (frequencies[vid] ?: 0) + frequency
        }
      }

      // Ordered prefix unions, per cell, in component order.
      val incrementsByCell = mutableMapOf<String, MutableList<Int>>()
      val prefixByCell = mutableMapOf<String, MutableSet<Long>>()
      for (edpName in componentEdpNames) {
        val sizeBefore: Map<String, Int> = prefixByCell.mapValues { it.value.size }
        for (vid in frequenciesByEdpAndFilter.getValue(edpName).getValue(label).keys) {
          val key = demographics.keyOf(vid) ?: continue
          prefixByCell.getOrPut(key) { mutableSetOf() }.add(vid)
        }
        for (key in unionByCell.keys) {
          val added = (prefixByCell[key]?.size ?: 0) - (sizeBefore[key] ?: 0)
          incrementsByCell.getOrPut(key) { mutableListOf() }.add(added)
        }
      }

      for ((key, frequencies) in unionByCell) {
        byCell.getOrPut(key) { mutableMapOf() }[label] =
          expectedMetrics(frequencies, params, maxFrequency, DEMOGRAPHIC_RELATIVE_TOLERANCE)
            .copy(
              populationSize = demographics.populationSizeByKey.getValue(key),
              stackedIncrementalReach =
                incrementsByCell.getValue(key).map { increment ->
                  rangeAround(increment.toDouble(), stackedTolerance, STACKED_RELATIVE_TOLERANCE)
                },
            )
      }
    }
    return byCell
  }

  /**
   * Expected metrics per week, keyed by demographic cell and filter.
   *
   * Weeks partition `[reportStart, reportStart + 7 * weekCount)`.
   */
  fun computeWeeklyRangesByDemographic(
    config: ImpressionTestDataConfig,
    eventGroupReferenceIds: Set<String>,
    populationSpec: PopulationSpec,
    reportStart: LocalDate,
    weekCount: Int,
    metricSpecConfig: MetricSpecConfig,
    maxFrequency: Int,
  ): Map<String, Map<String, List<ExpectedWeeklyMetrics>>> {
    val demographics = DemographicIndex(populationSpec)
    val weeklyFrequencies =
      weeklyFrequenciesByEdpAndFilter(
        config,
        eventGroupReferenceIds,
        populationSpec,
        reportStart,
        weekCount,
      )
    val params = metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
    val impressionParams = metricSpecConfig.impressionCountParams
    val reachTolerance = tolerance(params.reachPrivacyParams, params.vidSamplingInterval)

    val byCell = mutableMapOf<String, MutableMap<String, List<ExpectedWeeklyMetrics>>>()
    for (label in FILTER_PREDICATES.keys) {
      // Union across EDPs, bucketed by cell: a VID reached through more than one EDP in a week is
      // one person with the impressions of both. The per-EDP breakdown is kept alongside, since
      // the report also reports each component's own reach.
      val unionByCell = mutableMapOf<String, MutableMap<Long, IntArray>>()
      val perEdpByCell = mutableMapOf<String, MutableMap<String, MutableMap<Long, IntArray>>>()
      for ((edpName, byFilter) in weeklyFrequencies) {
        for ((vid, perWeek) in byFilter.getValue(label)) {
          val key = demographics.keyOf(vid) ?: continue
          val merged = unionByCell.getOrPut(key) { mutableMapOf() }
          val target = merged.getOrPut(vid) { IntArray(weekCount) }
          for (week in 0 until weekCount) {
            target[week] += perWeek[week]
          }
          perEdpByCell.getOrPut(key) { mutableMapOf() }.getOrPut(edpName) { mutableMapOf() }[vid] =
            perWeek
        }
      }

      for ((key, perVidInCell) in unionByCell) {
        val populationSize = demographics.populationSizeByKey.getValue(key)
        // Carried across weeks so each VID's cumulative frequency is accumulated once rather than
        // re-summed for every week.
        val cumulative = mutableMapOf<Long, Int>()
        val weeks =
          (0 until weekCount).map { week ->
            val nonCumulative = mutableMapOf<Long, Int>()
            for ((vid, perWeek) in perVidInCell) {
              if (perWeek[week] > 0) {
                nonCumulative[vid] = perWeek[week]
                cumulative[vid] = (cumulative[vid] ?: 0) + perWeek[week]
              }
            }
            // Every EDP, not only those with VIDs in this cell: the report returns a component
            // for each one, with zero reach where it contributes nothing.
            val componentReach =
              weeklyFrequencies.keys.associateWith { edpName ->
                val perVidInEdp = perEdpByCell[key]?.get(edpName).orEmpty()
                val reached = perVidInEdp.values.count { it[week] > 0 }.toDouble()
                rangeAround(reached, reachTolerance, WEEKLY_RELATIVE_TOLERANCE)
              }
            val cumulativeMetrics =
              expectedMetrics(cumulative.toMap(), params, maxFrequency, WEEKLY_RELATIVE_TOLERANCE)
                .copy(populationSize = populationSize)
            ExpectedWeeklyMetrics(
              cumulative = cumulativeMetrics,
              nonCumulative =
                expectedMetrics(nonCumulative, params, maxFrequency, WEEKLY_RELATIVE_TOLERANCE)
                  .copy(populationSize = populationSize),
              cumulativePercentReach = percentReach(cumulativeMetrics.reach, populationSize),
              nonCumulativeImpressions = impressions(nonCumulative, impressionParams),
              nonCumulativeAverageFrequency = averageFrequency(nonCumulative, impressionParams),
              componentNonCumulativeReach = componentReach,
            )
          }
        byCell.getOrPut(key) { mutableMapOf() }[label] = weeks
      }
    }

    // The report returns a result per (cell, filter) whether or not the dataset reaches anyone
    // there, so a cell missing a filter would otherwise surface as a lookup failure at assertion
    // time rather than as the dataset gap it is.
    for (key in demographics.keys) {
      val byFilter = byCell[key]
      require(byFilter != null && byFilter.keys.containsAll(FILTER_PREDICATES.keys)) {
        "No weekly expectations for cell $key under filters " +
          "${FILTER_PREDICATES.keys - byFilter?.keys.orEmpty()}: the dataset reaches nobody " +
          "there, but the report returns a result for every cell and filter"
      }
    }
    return byCell
  }

  /**
   * Returns the impression count per VID per week matching each filter, by EDP.
   *
   * Events outside `[reportStart, reportStart + 7 * weekCount)` are dropped.
   */
  private fun weeklyFrequenciesByEdpAndFilter(
    config: ImpressionTestDataConfig,
    eventGroupReferenceIds: Set<String>,
    populationSpec: PopulationSpec,
    reportStart: LocalDate,
    weekCount: Int,
  ): Map<String, Map<String, MutableMap<Long, IntArray>>> {
    val reportEnd = reportStart.plusWeeks(weekCount.toLong())
    val start = reportStart.atStartOfDay().toInstant(ZoneOffset.UTC)
    val endExclusive = reportEnd.atStartOfDay().toInstant(ZoneOffset.UTC)
    val timeRange: OpenEndRange<Instant> = start..<endExclusive

    val byEdp = mutableMapOf<String, Map<String, MutableMap<Long, IntArray>>>()
    for (eventGroup in config.eventGroupsList) {
      for (entityKeySpec in eventGroup.entityKeySpecsList) {
        val referenceId = "${entityKeySpec.entityType}-${entityKeySpec.entityId}"
        if (referenceId !in eventGroupReferenceIds) continue

        val frequenciesByFilter =
          byEdp.getOrPut(eventGroup.edpName) {
            FILTER_PREDICATES.keys.associateWith { mutableMapOf() }
          }
        val spec =
          ImpressionTestDataConfigs.resolveSyntheticEventGroupSpec(
            entityKeySpec.dataSpecResourcePath
          )
        for (shard in
          SyntheticDataGeneration.generateEvents(
            TestEvent.getDefaultInstance(),
            populationSpec,
            spec,
            timeRange,
          )) {
          val week = ChronoUnit.DAYS.between(reportStart, shard.localDate).toInt() / DAYS_PER_WEEK
          if (week !in 0 until weekCount) continue
          for (event in shard.labeledEvents) {
            for ((label, predicate) in FILTER_PREDICATES) {
              if (predicate(event.message)) {
                val frequencies = frequenciesByFilter.getValue(label)
                frequencies.getOrPut(event.vid) { IntArray(weekCount) }[week]++
              }
            }
          }
        }
      }
    }
    return byEdp
  }

  /** Expected impression count, truncated per VID at the maximum frequency per user. */
  private fun impressions(
    frequencies: Map<Long, Int>,
    impressionCountParams: MetricSpecConfig.ImpressionCountParams,
  ): ClosedFloatingPointRange<Double> {
    val maxPerUser = impressionCountParams.maximumFrequencyPerUser
    val count =
      frequencies.values.sumOf { if (maxPerUser > 0) minOf(it, maxPerUser) else it }.toDouble()
    // Sensitivity of an impression count is the maximum per user, so sigma scales with it.
    val impressionTolerance =
      maxOf(maxPerUser, 1) *
        tolerance(
          impressionCountParams.params.privacyParams,
          impressionCountParams.params.vidSamplingInterval,
        )
    return rangeAround(count, impressionTolerance, WEEKLY_RELATIVE_TOLERANCE)
  }

  /** Expected impressions divided by reach. */
  private fun averageFrequency(
    frequencies: Map<Long, Int>,
    impressionCountParams: MetricSpecConfig.ImpressionCountParams,
  ): ClosedFloatingPointRange<Double> {
    if (frequencies.isEmpty()) return 0.0..0.0
    val maxPerUser = impressionCountParams.maximumFrequencyPerUser
    val count =
      frequencies.values.sumOf { if (maxPerUser > 0) minOf(it, maxPerUser) else it }.toDouble()
    return rangeAround(count / frequencies.size, 0.0, RATIO_RELATIVE_TOLERANCE)
  }

  /**
   * Expected reach as a percentage of [populationSize], which is how the report reports it.
   *
   * Scaled from [reach] rather than given its own tolerance: the report derives percent reach from
   * the same noised reach by an unnoised constant, so the band is that transform of [reach]'s.
   */
  private fun percentReach(
    reach: ClosedFloatingPointRange<Double>,
    populationSize: Long,
  ): ClosedFloatingPointRange<Double> {
    if (populationSize == 0L) return 0.0..0.0
    val scale = PERCENT / populationSize
    return (reach.start * scale)..(reach.endInclusive * scale)
  }

  private fun expectedMetrics(
    frequencies: Map<Long, Int>,
    reachAndFrequencyParams: MetricSpecConfig.ReachAndFrequencySamplingAndPrivacyParams,
    maxFrequency: Int,
    relativeTolerance: Double = RELATIVE_TOLERANCE,
  ): ExpectedMetrics {
    val reachTolerance =
      tolerance(
        reachAndFrequencyParams.reachPrivacyParams,
        reachAndFrequencyParams.vidSamplingInterval,
      )

    return ExpectedMetrics(
      reach = rangeAround(frequencies.size.toDouble(), reachTolerance, relativeTolerance),
      kPlusReach =
        (1..maxFrequency).map { k ->
          rangeAround(
            frequencies.values.count { it >= k }.toDouble(),
            reachTolerance,
            relativeTolerance,
          )
        },
    )
  }

  /** Number of VIDs the population spec declares, which the report returns unnoised. */
  fun populationSize(populationSpec: PopulationSpec): Long =
    populationSpec.subpopulationsList.sumOf { subpopulation ->
      subpopulation.vidRangesList.sumOf { it.endVidInclusive - it.startVid + 1 }
    }

  private fun rangeAround(
    expected: Double,
    tolerance: Double,
    relativeTolerance: Double = RELATIVE_TOLERANCE,
  ): ClosedFloatingPointRange<Double> {
    // The post-processor shifts values by an amount that scales with the report's magnitudes, which
    // the additive measurement noise does not cover.
    val margin = maxOf(tolerance, relativeTolerance * expected)
    return (expected - margin)..(expected + margin)
  }

  /**
   * Returns the impression count per VID matching each impression qualification filter, by EDP.
   *
   * Generation is bounded to the reporting interval, since a segment's flight can run far wider
   * than the interval reported on. Each EDP is generated once; the cross-publisher expectation
   * merges these, deduplicating the VIDs a segment reaches through more than one EDP while summing
   * their impressions.
   */
  private fun frequenciesByEdpAndFilter(
    config: ImpressionTestDataConfig,
    eventGroupReferenceIds: Set<String>,
    populationSpec: PopulationSpec,
    reportStart: LocalDate,
    reportEnd: LocalDate,
  ): Map<String, Map<String, Map<Long, Int>>> {
    val start = reportStart.atStartOfDay().toInstant(ZoneOffset.UTC)
    val endExclusive = reportEnd.atStartOfDay().toInstant(ZoneOffset.UTC)
    val timeRange: OpenEndRange<Instant> = start..<endExclusive

    val byEdp = mutableMapOf<String, Map<String, MutableMap<Long, Int>>>()
    for (eventGroup in config.eventGroupsList) {
      for (entityKeySpec in eventGroup.entityKeySpecsList) {
        val referenceId = "${entityKeySpec.entityType}-${entityKeySpec.entityId}"
        if (referenceId !in eventGroupReferenceIds) continue

        val frequenciesByFilter =
          byEdp.getOrPut(eventGroup.edpName) {
            FILTER_PREDICATES.keys.associateWith { mutableMapOf() }
          }
        val spec =
          ImpressionTestDataConfigs.resolveSyntheticEventGroupSpec(
            entityKeySpec.dataSpecResourcePath
          )
        for (shard in
          SyntheticDataGeneration.generateEvents(
            TestEvent.getDefaultInstance(),
            populationSpec,
            spec,
            timeRange,
          )) {
          for (event in shard.labeledEvents) {
            for ((label, predicate) in FILTER_PREDICATES) {
              if (predicate(event.message)) {
                val frequencies = frequenciesByFilter.getValue(label)
                frequencies[event.vid] = (frequencies[event.vid] ?: 0) + 1
              }
            }
          }
        }
      }
    }
    return byEdp
  }

  /**
   * Margin of error for a metric noised with the given params.
   *
   * The BasicReport exposes neither the protocol nor the noise mechanism of the Measurements behind
   * a line item, so the variance cannot be computed the way the Measurement tests do. This assumes
   * continuous Gaussian noise, which is what the EDP Aggregator applies, and uses the closed form:
   * `sigma = sqrt(2 * ln(1.25 / delta)) / (epsilon * sampling_width)`.
   */
  private fun tolerance(
    privacyParams: MetricSpecConfig.DifferentialPrivacyParams,
    samplingInterval: MetricSpecConfig.VidSamplingInterval,
  ): Double {
    val epsilon = privacyParams.epsilon
    val delta = privacyParams.delta
    val width = samplingInterval.fixedStart.width
    require(epsilon > 0 && delta > 0 && width > 0) {
      "Invalid noise params: epsilon=$epsilon delta=$delta width=$width"
    }
    val sigma = sqrt(2.0 * ln(1.25 / delta)) / (epsilon * width)
    return CONFIDENCE_INTERVAL_MULTIPLIER * sigma
  }

  /**
   * Predicates matching the impression qualification filters the report requests.
   *
   * `ami` admits every media type; `mrc` is display-only at the viewability thresholds its config
   * declares; the custom filter is video-only.
   */
  private val FILTER_PREDICATES: Map<String, (TestEvent) -> Boolean> =
    linkedMapOf(
      ReportingUserSimulator.AMI_FILTER_ID to
        { event: TestEvent ->
          event.hasVideo() || event.hasDisplay() || event.hasOther()
        },
      ReportingUserSimulator.MRC_FILTER_ID to
        { event: TestEvent ->
          event.hasDisplay() && event.display.viewableFraction in MRC_VIEWABLE_FRACTIONS
        },
      ReportingUserSimulator.CUSTOM_VIDEO_FILTER_LABEL to
        { event: TestEvent ->
          event.hasVideo() &&
            event.video.completedFraction in ReportingUserSimulator.CUSTOM_VIDEO_COMPLETED_FRACTIONS
        },
    )

  // For a 99.9999% confidence interval, matching MeasurementConsumerSimulator.
  private const val CONFIDENCE_INTERVAL_MULTIPLIER = 5.0

  /** Minimum margin, as a fraction of the expected value. */
  private const val RELATIVE_TOLERANCE = 0.15

  /**
   * Minimum margin for a single demographic cell.
   *
   * A cell carries roughly a sixth of the reach the ungrouped metrics do, so the same absolute
   * noise is a far larger share of it.
   */
  private const val DEMOGRAPHIC_RELATIVE_TOLERANCE = 0.35

  /** Minimum margin for one stacked incremental contribution. */
  private const val DAYS_PER_WEEK = 7

  private const val PERCENT = 100.0

  /**
   * Relative tolerance for weekly values.
   *
   * A week carries roughly a ninth of the interval's reach while the additive noise per metric is
   * unchanged, so weekly values sit closer to the noise floor than the whole-interval ones and need
   * more room than [DEMOGRAPHIC_RELATIVE_TOLERANCE].
   */
  private const val WEEKLY_RELATIVE_TOLERANCE = 0.50

  /**
   * Relative tolerance for metrics the report derives as a ratio of two noised values.
   *
   * Both numerator and denominator carry their own noise, so the quotient's error is wider than
   * either, and no closed form is available from the params the BasicReport exposes.
   */
  private const val RATIO_RELATIVE_TOLERANCE = 0.60

  private const val STACKED_RELATIVE_TOLERANCE = 0.50

  /** Two independently noised prefix reaches are subtracted, so their variances add. */
  private const val STACKED_NOISE_MULTIPLIER = 1.4142135623730951

  private val MRC_VIEWABLE_FRACTIONS = setOf(0.5f, 0.75f, 1.0f)
}
