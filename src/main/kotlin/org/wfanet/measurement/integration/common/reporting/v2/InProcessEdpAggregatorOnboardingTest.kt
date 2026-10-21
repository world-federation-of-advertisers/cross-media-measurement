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
import com.google.devtools.build.runfiles.Runfiles
import java.nio.file.Path
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

/** Integration test for the complete TrusTEE EDP onboarding and reporting flow. */
abstract class InProcessEdpAggregatorOnboardingTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
  secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
  accessServicesFactory: AccessServicesFactory,
  reportingDataServicesProviderRule: ProviderRule<Services>,
) :
  InProcessEdpAggregatorLifeOfAReportTest(
    kingdomDataServicesRule = kingdomDataServicesRule,
    duchyDependenciesRule = duchyDependenciesRule,
    secureComputationDatabaseAdmin = secureComputationDatabaseAdmin,
    accessServicesFactory = accessServicesFactory,
    reportingDataServicesProviderRule = reportingDataServicesProviderRule,
    duchyNames = ALL_DUCHY_NAMES,
    hmssEnabled = false,
    trusTeeEnabled = true,
    multiEdpDisplayNames = setOf("edp1", "edp2"),
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
    val impressionVerifierResult =
      runVerifier(deployJarPath("VerifyVidLabeledImpressions_deploy.jar"), impressionVerifierArgs)
    assertWithMessage("Verifier output:\n${impressionVerifierResult.output}")
      .that(impressionVerifierResult.exitCode)
      .isEqualTo(0)
    assertThat(impressionVerifierResult.output).contains("Blobs processed: ${metadataUris.size}")
    val totalImpressionsMatch = TOTAL_IMPRESSIONS_REGEX.find(impressionVerifierResult.output)
    assertWithMessage("Verifier output:\n${impressionVerifierResult.output}")
      .that(totalImpressionsMatch)
      .isNotNull()
    assertThat(checkNotNull(totalImpressionsMatch).groupValues[1].toLong()).isGreaterThan(0L)

    val eventGroupFiles = exportEventGroups(edpDisplayNames)
    val eventGroupVerifierArgs =
      eventGroupFiles.values
        .sortedBy { it.path }
        .flatMap { file -> listOf("--event-groups-file", file.absolutePath) }
        .toTypedArray()
    val eventGroupVerifierResult =
      runVerifier(deployJarPath("VerifyEventGroups_deploy.jar"), eventGroupVerifierArgs)
    assertWithMessage("Verifier output:\n${eventGroupVerifierResult.output}")
      .that(eventGroupVerifierResult.exitCode)
      .isEqualTo(0)
    for (eventGroupFile in eventGroupFiles.values) {
      assertThat(eventGroupVerifierResult.output).contains("PASS: ${eventGroupFile.absolutePath}:")
    }

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
    assertNoNoiseResults(
      completedBasicReport,
      expectedCrossPublisherReach = EXPECTED_TRUSTEE_CROSS_PUBLISHER_REACH,
      expectedCrossPublisherImpressions = EXPECTED_TRUSTEE_CROSS_PUBLISHER_IMPRESSIONS,
      expectedKPlusReach = EXPECTED_TRUSTEE_K_PLUS_REACH,
      expectedEdpSpec1Reach = EXPECTED_TRUSTEE_EDP_SPEC1_REACH,
      expectedEdpSpec2Reach = EXPECTED_TRUSTEE_EDP_SPEC2_REACH,
    )
    assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
  }

  private fun runVerifier(jarPath: Path, args: Array<String>): VerifierResult {
    val javaBinary =
      Paths.get(checkNotNull(System.getProperty("java.home")), "bin", "java").toString()
    val command = buildList {
      add(javaBinary)
      add("-jar")
      add(jarPath.toString())
      addAll(args.asList())
    }
    val process = ProcessBuilder(command).redirectErrorStream(true).start()
    val output = process.inputStream.bufferedReader().use { it.readText() }
    return VerifierResult(exitCode = process.waitFor(), output = output)
  }

  private fun deployJarPath(fileName: String): Path =
    checkNotNull(
      RUNFILES.getRuntimePath(
        Paths.get(
          "wfa_measurement_system",
          "src",
          "main",
          "kotlin",
          "org",
          "wfanet",
          "measurement",
          "edpaggregator",
          "tools",
          fileName,
        )
      )
    ) {
      "$fileName not found in runfiles"
    }

  private data class VerifierResult(val exitCode: Int, val output: String)

  companion object {
    private val RUNFILES = Runfiles.preload().withSourceRepository("")
    private val TOTAL_IMPRESSIONS_REGEX = Regex("""Total impressions decrypted:\s+(\d+)""")
    private const val EXPECTED_TRUSTEE_CROSS_PUBLISHER_REACH = 5330L
    private const val EXPECTED_TRUSTEE_CROSS_PUBLISHER_IMPRESSIONS = 8860L
    private val EXPECTED_TRUSTEE_K_PLUS_REACH = listOf(5330L, 2572L, 647L, 311L, 0L)
    private const val EXPECTED_TRUSTEE_EDP_SPEC1_REACH = 3937L
    private const val EXPECTED_TRUSTEE_EDP_SPEC2_REACH = 3638L
  }
}
