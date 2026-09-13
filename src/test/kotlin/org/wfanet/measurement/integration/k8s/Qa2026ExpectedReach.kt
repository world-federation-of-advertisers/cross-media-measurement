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
 * Expected reach for the QA 2026 media-type and impression-qualification-filter report, derived from
 * the synthetic specs rather than from the impressions the EDP Aggregator reads.
 *
 * Reach is the number of distinct VIDs matching a filter over the reporting interval, unscaled: the
 * measured value is already scaled back up from the VID sampling interval, which affects only the
 * tolerance.
 */
object Qa2026ExpectedReach {

  /**
   * Returns the acceptable reach range per impression qualification filter label for each of the two
   * result groups.
   *
   * @param config QA 2026 config restricted to the provisioned EDPs
   * @param singleEdpName the EDP the single-EDP result group reports on
   * @param populationSpec the QA 2026 population spec
   * @param reportStart first event date, inclusive
   * @param reportEnd last event date, inclusive
   * @param metricSpecConfig the deployed metric spec config, for the noise parameters
   */
  fun computeRangesByGroupAndFilter(
    config: ImpressionTestDataConfig,
    singleEdpName: String,
    populationSpec: PopulationSpec,
    reportStart: LocalDate,
    reportEnd: LocalDate,
    metricSpecConfig: MetricSpecConfig,
  ): Map<String, Map<String, ClosedFloatingPointRange<Double>>> {
    val singleEdpVids: Map<String, Set<Long>> =
      vidsByFilter(config, populationSpec, reportStart, reportEnd) { it == singleEdpName }
    val allEdpVids: Map<String, Set<Long>> =
      vidsByFilter(config, populationSpec, reportStart, reportEnd) { true }

    val singleTolerance = reachTolerance(metricSpecConfig.reachParams.singleDataProviderParams)
    val multipleTolerance = reachTolerance(metricSpecConfig.reachParams.multipleDataProviderParams)

    return mapOf(
      ReportingUserSimulator.SINGLE_EDP_GROUP_TITLE to
        singleEdpVids.mapValues { (_, vids) -> rangeAround(vids.size, singleTolerance) },
      ReportingUserSimulator.CROSS_PUB_GROUP_TITLE to
        allEdpVids.mapValues { (_, vids) -> rangeAround(vids.size, multipleTolerance) },
    )
  }

  private fun rangeAround(expected: Int, tolerance: Double): ClosedFloatingPointRange<Double> =
    (expected - tolerance)..(expected + tolerance)

  /**
   * Returns the distinct VIDs matching each impression qualification filter, over the EDPs
   * [edpNameFilter] admits.
   */
  private fun vidsByFilter(
    config: ImpressionTestDataConfig,
    populationSpec: PopulationSpec,
    reportStart: LocalDate,
    reportEnd: LocalDate,
    edpNameFilter: (String) -> Boolean,
  ): Map<String, Set<Long>> {
    val start = reportStart.atStartOfDay().toInstant(ZoneOffset.UTC)
    val endExclusive = reportEnd.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC)

    val vidsByFilter: Map<String, MutableSet<Long>> =
      FILTER_PREDICATES.keys.associateWith { mutableSetOf<Long>() }

    for (eventGroup in config.eventGroupsList) {
      if (!edpNameFilter(eventGroup.edpName)) continue
      for (entityKeySpec in eventGroup.entityKeySpecsList) {
        val spec =
          ImpressionTestDataConfigs.resolveSyntheticEventGroupSpec(entityKeySpec.dataSpecResourcePath)
        for (shard in
          SyntheticDataGeneration.generateEvents(
            TestEvent.getDefaultInstance(),
            populationSpec,
            spec,
          )) {
          for (event in shard.labeledEvents) {
            if (event.timestamp < start || event.timestamp >= endExclusive) continue
            for ((label, predicate) in FILTER_PREDICATES) {
              if (predicate(event.message)) {
                vidsByFilter.getValue(label).add(event.vid)
              }
            }
          }
        }
      }
    }
    return vidsByFilter
  }

  /**
   * Margin of error for a reach metric with the given params.
   *
   * The BasicReport exposes neither the protocol nor the noise mechanism of the Measurements behind
   * a line item, so the variance cannot be computed the way the Measurement tests do. This assumes
   * continuous Gaussian noise, which is what the EDP Aggregator applies, and uses the closed form:
   * `sigma = sqrt(2 * ln(1.25 / delta)) / (epsilon * sampling_width)`.
   */
  private fun reachTolerance(params: MetricSpecConfig.SamplingAndPrivacyParams): Double {
    val epsilon = params.privacyParams.epsilon
    val delta = params.privacyParams.delta
    val width = params.vidSamplingInterval.fixedStart.width
    require(epsilon > 0 && delta > 0 && width > 0) {
      "Invalid reach params: epsilon=$epsilon delta=$delta width=$width"
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
      ReportingUserSimulator.AMI_FILTER_ID to { event: TestEvent ->
        event.hasVideo() || event.hasDisplay() || event.hasOther()
      },
      ReportingUserSimulator.MRC_FILTER_ID to { event: TestEvent ->
        event.hasDisplay() && event.display.viewableFraction in MRC_VIEWABLE_FRACTIONS
      },
      ReportingUserSimulator.CUSTOM_VIDEO_FILTER_LABEL to { event: TestEvent -> event.hasVideo() },
    )

  // For a 99.9999% confidence interval, matching MeasurementConsumerSimulator.
  private const val CONFIDENCE_INTERVAL_MULTIPLIER = 5.0

  private val MRC_VIEWABLE_FRACTIONS = setOf(0.5f, 0.75f, 1.0f)
}
