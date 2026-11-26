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
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.internal.reporting.v2.BasicReport as InternalBasicReport
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportsService
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

/**
 * Tests a [BasicReport] computed by TrusTEE v2 multimeasurements end to end: the Reporting API
 * accepts it on the multimeasurement path, `BasicReportsMeasurementsJob` creates its CMMS
 * multimeasurements, the EDP Aggregator and the TrusTEE v2 Duchy compute them, and
 * `BasicReportsMultimeasurementResultsJob` writes their results for the report post-processor.
 */
abstract class InProcessEdpAggregatorMultimeasurementReportTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
  secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
  accessServicesFactory: AccessServicesFactory,
  reportingDataServicesProviderRule: ProviderRule<Services>,
) :
  InProcessEdpAggregatorLifeOfAReportTest(
    kingdomDataServicesRule,
    duchyDependenciesRule,
    secureComputationDatabaseAdmin,
    accessServicesFactory,
    reportingDataServicesProviderRule,
    hmssEnabled = false,
    trusTeeEnabled = true,
    multiEdpDisplayNames = setOf("edp1", "edp2"),
    multimeasurementMode = BasicReportsService.MultimeasurementMode.ALL,
  ) {

  @Test
  fun `multimeasurement basic report has the expected result`(): Unit = runBlocking {
    val eventGroups = getMultiEdpEventGroups()
    check(eventGroups.size > 1)
    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        eventGroups,
        "multimeasurement-campaign",
        "multimeasurement-basicreport",
        includeIqfFilter = false,
        // TODO(world-federation-of-advertisers/cross-media-measurement#4532): Request stacked
        // incremental reach once multimeasurements compute it.
        stackedIncrementalReach = false,
      )
    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(createBasicReportRequest)

    executeBasicReportsMeasurementsJob()

    assertWithMessage("TrusTEE v2 measurements")
      .that(
        listMeasurements().filter { measurement ->
          measurement.protocolConfig.protocolsList.any { it.hasTrusTeeV2() }
        }
      )
      .isNotEmpty()
    assertThat(executeBasicReportsMultimeasurementResultsJob(createdBasicReport.name))
      .isEqualTo(InternalBasicReport.State.UNPROCESSED_RESULTS_READY)

    executeReportProcessorJob(enableValidation = true, enableCorrection = true)

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })
    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
    val metricSet = completedBasicReport.resultGroupsList.single().resultsList.single().metricSet
    assertThat(metricSet.reportingUnit.cumulative.reach).isEqualTo(EXPECTED_CROSS_PUBLISHER_REACH)
    assertThat(metricSet.reportingUnit.cumulative.impressions)
      .isEqualTo(EXPECTED_CROSS_PUBLISHER_IMPRESSIONS)
    assertThat(metricSet.componentsList.map { it.value.cumulative.reach })
      .containsExactly(EXPECTED_EDP_SPEC1_REACH, EXPECTED_EDP_SPEC2_REACH)
  }
}
