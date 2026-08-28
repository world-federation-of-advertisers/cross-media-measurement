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

package org.wfanet.measurement.integration.common.reporting.v2

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.ResultGroup
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

/**
 * Integration tests for Direct results noised with DETERMINISTIC_TRUNCATED_LAPLACE.
 *
 * The EDPs select the mechanism through `ResultsFulfillerParams.noise_type`, which is the whole
 * migration surface: no schema or data changes, and reverting the setting runs the previous path on
 * the next measurement. [InProcessEdpAggregatorDirectOnlyReportTest] runs the same reports with
 * NoiseType.NONE, so the pair covers the flip in both directions.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests.
 */
abstract class InProcessEdpAggregatorDirectDeterministicNoiseTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
  secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
  accessServicesFactory: AccessServicesFactory,
  reportingDataServicesProviderRule: ProviderRule<Services>,
  duchyNames: List<String> = ALL_DUCHY_NAMES,
) :
  InProcessEdpAggregatorLifeOfAReportTest(
    kingdomDataServicesRule,
    duchyDependenciesRule,
    secureComputationDatabaseAdmin,
    accessServicesFactory,
    reportingDataServicesProviderRule,
    duchyNames,
    hmssEnabled = false,
    trusTeeEnabled = false,
    directNoiseType = ResultsFulfillerParams.NoiseParams.NoiseType.DETERMINISTIC_TRUNCATED_LAPLACE,
  ) {

  @Test
  fun `direct results are within the noise bound of the unnoised values`() = runBlocking {
    val cumulative = runReport("deterministic-bound")

    assertWithinNoiseBound(
      "reach",
      cumulative.reach,
      EXPECTED_SINGLE_EDP_SPEC2_REACH,
      REACH_SENSITIVITY,
    )
    for ((index, expected) in EXPECTED_SINGLE_EDP_SPEC2_K_PLUS_REACH.withIndex()) {
      assertWithinNoiseBound(
        "${index + 1}+ reach",
        cumulative.kPlusReachList[index],
        expected,
        K_PLUS_SENSITIVITY,
      )
    }
    assertWithinNoiseBound(
      "impressions",
      cumulative.impressions,
      EXPECTED_SINGLE_EDP_SPEC2_IMPRESSIONS,
      IMPRESSION_SENSITIVITY,
    )
  }

  @Test
  fun `direct results record the deterministic mechanism`() = runBlocking {
    val basicReportName = runBasicReport("deterministic-mechanism")

    val results = decryptedResults(getMeasurementsForBasicReport(basicReportName))

    // Each result type is asserted non-empty first, so a type dropping out of the report fails here
    // rather than passing vacuously through its mechanism check.
    val reachMechanisms = results.filter { it.hasReach() }.map { it.reach.noiseMechanism }
    val frequencyMechanisms =
      results.filter { it.hasFrequency() }.map { it.frequency.noiseMechanism }
    val impressionMechanisms =
      results.filter { it.hasImpression() }.map { it.impression.noiseMechanism }

    assertWithMessage("reach results").that(reachMechanisms).isNotEmpty()
    assertWithMessage("frequency results").that(frequencyMechanisms).isNotEmpty()
    assertWithMessage("impression results").that(impressionMechanisms).isNotEmpty()

    for ((label, mechanisms) in
      listOf(
        "reach" to reachMechanisms,
        "frequency" to frequencyMechanisms,
        "impression" to impressionMechanisms,
      )) {
      assertWithMessage("$label noise mechanisms")
        .that(mechanisms)
        .containsExactlyElementsIn(
          List(mechanisms.size) { ProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE }
        )
    }
  }

  @Test
  fun `rerunning the same report draws the same noise`() = runBlocking {
    val first = runReport("deterministic-rerun-1")
    val second = runReport("deterministic-rerun-2")

    assertWithMessage("reach").that(second.reach).isEqualTo(first.reach)
    assertWithMessage("impressions").that(second.impressions).isEqualTo(first.impressions)
    assertWithMessage("k+ reach")
      .that(second.kPlusReachList)
      .containsExactlyElementsIn(first.kPlusReachList)
      .inOrder()
  }

  /**
   * Runs a single-EDP basic report over the reference-id-only event groups and returns its total.
   */
  private suspend fun runReport(label: String): ResultGroup.MetricSet.BasicMetricSet {
    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = runBasicReport(label) })

    return completedBasicReport.resultGroupsList
      .single()
      .resultsList
      .single { it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL }
      .metricSet
      .reportingUnit
      .cumulative
  }

  /** Runs a single-EDP basic report over the reference-id-only event groups, returning its name. */
  private suspend fun runBasicReport(label: String): String {
    val eventGroups = getReferenceIdOnlyEventGroups()
    check(eventGroups.isNotEmpty()) { "No reference-ID-only event groups found" }

    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(
          buildCreateBasicReportRequest(
            eventGroups,
            "$label-campaign",
            "$label-basicreport",
            includeIqfFilter = false,
          )
        )

    executeBasicReportsReportsJob(createdBasicReport.name)
    executeReportProcessorJob()

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertStructuralResults(completedBasicReport)
    assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))

    return completedBasicReport.name
  }

  companion object {
    /** One VID moves reach by 1. */
    private const val REACH_SENSITIVITY = 1.0

    /**
     * One VID moves the impression count by the maximum frequency per user, which
     * `impressionCountParams` in the metric spec config sets to 60.
     */
    private const val IMPRESSION_SENSITIVITY = 60.0

    /**
     * A k+ value is a sum over frequency buckets, each carrying its own draw, normalized against a
     * denominator that carries all of them. Bounding it by the histogram width is conservative.
     */
    private const val K_PLUS_SENSITIVITY = 5.0
  }
}
