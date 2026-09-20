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

package org.wfanet.measurement.integration.k8s

import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.math.ln
import kotlin.math.sqrt
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.integration.common.ImpressionTestDataConfigs
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration
import org.wfanet.measurement.loadtest.reporting.ReportingUserSimulator

/**
 * Expected reach for the QA 2026 media-type and impression-qualification-filter report, derived
 * from the synthetic specs rather than from the impressions the EDP Aggregator reads.
 *
 * Reach is the number of distinct VIDs matching a filter over the reporting interval, unscaled: the
 * measured value is already scaled back up from the VID sampling interval, which affects only the
 * tolerance.
 */
object Qa2026ExpectedReach {

  /** Acceptable range for every metric the report requests for one line item. */
  data class ExpectedMetrics(
    val reach: ClosedFloatingPointRange<Double>,
    val impressions: ClosedFloatingPointRange<Double>,
    val averageFrequency: ClosedFloatingPointRange<Double>,
    /** Indexed by frequency - 1, so element `i` is the reach at frequency `i + 1` or more. */
    val kPlusReach: List<ClosedFloatingPointRange<Double>>,
  )

  /**
   * Returns the acceptable metric ranges per impression qualification filter label for each of the
   * two result groups.
   *
   * @param config QA 2026 config restricted to the provisioned EDPs
   * @param singleEdpName the EDP the single-EDP result group reports on
   * @param populationSpec the QA 2026 population spec
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
    val impressionParams = metricSpecConfig.impressionCountParams.params

    return mapOf(
      ReportingUserSimulator.SINGLE_EDP_GROUP_TITLE to
        singleEdpFrequencies.mapValues { (_, frequencies) ->
          expectedMetrics(
            frequencies,
            reachAndFrequencyParams.singleDataProviderParams,
            impressionParams,
            maxFrequency,
          )
        },
      ReportingUserSimulator.CROSS_PUB_GROUP_TITLE to
        allEdpFrequencies.mapValues { (_, frequencies) ->
          expectedMetrics(
            frequencies,
            reachAndFrequencyParams.multipleDataProviderParams,
            impressionParams,
            maxFrequency,
          )
        },
    )
  }

  private fun expectedMetrics(
    frequencies: Map<Long, Int>,
    reachAndFrequencyParams: MetricSpecConfig.ReachAndFrequencySamplingAndPrivacyParams,
    impressionParams: MetricSpecConfig.SamplingAndPrivacyParams,
    maxFrequency: Int,
  ): ExpectedMetrics {
    val reachTolerance =
      tolerance(
        reachAndFrequencyParams.reachPrivacyParams,
        reachAndFrequencyParams.vidSamplingInterval,
      )
    val impressionTolerance =
      tolerance(impressionParams.privacyParams, impressionParams.vidSamplingInterval)

    val reach = frequencies.size
    val impressions = frequencies.values.sumOf { it.toLong() }
    val reachRange = rangeAround(reach.toDouble(), reachTolerance)
    val impressionRange = rangeAround(impressions.toDouble(), impressionTolerance)

    return ExpectedMetrics(
      reach = reachRange,
      impressions = impressionRange,
      // Ratio of two independently noised values, so the bound is the widest the ranges allow.
      averageFrequency =
        (impressionRange.start / reachRange.endInclusive)..(impressionRange.endInclusive /
            maxOf(reachRange.start, 1.0)),
      kPlusReach =
        (1..maxFrequency).map { k ->
          rangeAround(frequencies.values.count { it >= k }.toDouble(), reachTolerance)
        },
    )
  }

  /** Number of VIDs the population spec declares, which the report returns unnoised. */
  fun populationSize(populationSpec: PopulationSpec): Long =
    populationSpec.subpopulationsList.sumOf { subpopulation ->
      subpopulation.vidRangesList.sumOf { it.endVidInclusive - it.startVid + 1 }
    }

  private fun rangeAround(expected: Double, tolerance: Double): ClosedFloatingPointRange<Double> {
    // The post-processor shifts values by an amount that scales with the report's magnitudes, which
    // the additive measurement noise does not cover.
    val margin = maxOf(tolerance, RELATIVE_TOLERANCE * expected)
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
            event.video.viewableFraction in ReportingUserSimulator.CUSTOM_VIDEO_VIEWABLE_FRACTIONS
        },
    )

  // For a 99.9999% confidence interval, matching MeasurementConsumerSimulator.
  private const val CONFIDENCE_INTERVAL_MULTIPLIER = 5.0

  /** Minimum margin, as a fraction of the expected value. */
  private const val RELATIVE_TOLERANCE = 0.1

  private val MRC_VIEWABLE_FRACTIONS = setOf(0.5f, 0.75f, 1.0f)
}
