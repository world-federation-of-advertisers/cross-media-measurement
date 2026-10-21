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
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.edpaggregator.tools.VerifyEventGroups
import org.wfanet.measurement.edpaggregator.tools.VerifyVidLabeledImpressions
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import picocli.CommandLine

/** Integration test for the complete TrusTEE EDP onboarding and reporting flow. */
abstract class InProcessEdpAggregatorOnboardingTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
  secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
  accessServicesFactory: AccessServicesFactory,
  reportingDataServicesProviderRule: ProviderRule<Services>,
) :
  InProcessEdpAggregatorTrusTeeThresholdTest(
    kingdomDataServicesRule = kingdomDataServicesRule,
    duchyDependenciesRule = duchyDependenciesRule,
    secureComputationDatabaseAdmin = secureComputationDatabaseAdmin,
    accessServicesFactory = accessServicesFactory,
    reportingDataServicesProviderRule = reportingDataServicesProviderRule,
  ) {

  @Test
  fun `verified onboarding exports produce the expected TrusTee report`() = runBlocking {
    val edpDisplayNames = setOf("edp1", "edp2")

    val metadataUris = exportLabeledImpressions(edpDisplayNames)
    val impressionVerifierArgs =
      buildList {
          add("--kms-type=FAKE")
          add("--kek-uri=${fakeKmsKeyUri()}")
          add("--fake-kek-keyset-file=${writeFakeKmsKeysetFile().absolutePath}")
          add("--local-storage-path=${tempPath.toFile().absolutePath}")
          for (metadataUri in metadataUris) {
            add("--metadata-uri=$metadataUri")
          }
        }
        .toTypedArray()
    val impressionVerifier = VerifyVidLabeledImpressions()
    assertThat(CommandLine(impressionVerifier).execute(*impressionVerifierArgs)).isEqualTo(0)
    val verificationResult = checkNotNull(impressionVerifier.lastResult)
    assertThat(verificationResult.totalBlobsProcessed).isEqualTo(metadataUris.size)
    assertThat(verificationResult.totalImpressions).isGreaterThan(0L)

    val eventGroupFiles = exportEventGroups(edpDisplayNames)
    val eventGroupVerifierArgs =
      eventGroupFiles.values
        .sortedBy { it.path }
        .flatMap { file -> listOf("--event-groups-file", file.absolutePath) }
        .toTypedArray()
    assertThat(CommandLine(VerifyEventGroups()).execute(*eventGroupVerifierArgs)).isEqualTo(0)

    for ((edpDisplayName, eventGroupFile) in eventGroupFiles) {
      val sourceEventGroups = readEventGroups(eventGroupFile)
      val mappedEventGroups = syncEventGroups(edpDisplayName, sourceEventGroups, emptyList())
      assertThat(mappedEventGroups).hasSize(sourceEventGroups.size)
    }

    val eventGroups = getCreativeIdOnlyEventGroups()
    assertThat(eventGroups).hasSize(2)
    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        eventGroups,
        "onboarding-campaign",
        "onboarding-basicreport",
        includeIqfFilter = false,
      )
    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(createBasicReportRequest)

    executeBasicReportsReportsJob(createdBasicReport.name)
    executeReportProcessorJob()

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })
    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertStructuralResults(completedBasicReport)
    assertTrusTeeMetricResults(completedBasicReport)
    assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
  }
}
