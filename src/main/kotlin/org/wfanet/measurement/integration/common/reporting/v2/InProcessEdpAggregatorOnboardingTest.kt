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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.devtools.build.runfiles.Runfiles
import com.google.type.interval
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.MediaType
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.mappedEventGroup
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.entityKey
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
  override val edpsWithDeferredUploads: Set<String>
    get() = setOf("edp1", "edp2")

  @Test
  fun `verified onboarding exports produce the expected TrusTee report`() = runBlocking {
    val edpDisplayNames = setOf("edp1", "edp2")
    for (edpDisplayName in edpDisplayNames) {
      assertThat(listCmmsEventGroups(edpDisplayName, listOf("campaign", "ad_group", "creative-id")))
        .isEmpty()
      assertThat(listImpressionMetadata(edpDisplayName)).isEmpty()
      assertThat(getDataAvailabilityIntervals(edpDisplayName)).isEmpty()
    }

    val metadataUrisByEdp =
      edpDisplayNames.sorted().associateWith { edpDisplayName ->
        exportLabeledImpressions(setOf(edpDisplayName))
      }
    val metadataUris = metadataUrisByEdp.values.flatten()
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

    for ((edpDisplayName, edpMetadataUris) in metadataUrisByEdp) {
      assertThat(listImpressionMetadata(edpDisplayName)).isEmpty()
      assertThat(getDataAvailabilityIntervals(edpDisplayName)).isEmpty()
      val expectedDoneUris =
        edpMetadataUris.map { "${it.substringBeforeLast('/')}/done" }.distinct()
      for (doneUri in expectedDoneUris) {
        assertThat(Files.exists(tempPath.resolve(URI(doneUri).path.removePrefix("/")))).isFalse()
      }
      val publicationResult = publishImpressions(edpDisplayName, edpMetadataUris)
      assertThat(publicationResult.outcomes.keys).containsExactlyElementsIn(expectedDoneUris)
      assertThat(publicationResult.outcomes.values.toSet())
        .containsExactly(DataAvailabilitySync.Outcome.PUBLISHED)

      val importedMetadata = listImpressionMetadata(edpDisplayName)
      assertThat(importedMetadata.map { it.blobUri }).containsExactlyElementsIn(edpMetadataUris)
      val sidecarsByUri =
        edpMetadataUris.associateWith { metadataUri ->
          BlobDetails.parseFrom(
            Files.readAllBytes(tempPath.resolve(URI(metadataUri).path.removePrefix("/")))
          )
        }
      val expectedCustomCreateTimes = buildMap {
        for ((metadataUri, sidecar) in sidecarsByUri) {
          val startTime = sidecar.interval.startTime.toInstant()
          put(metadataUri, startTime)
          put(sidecar.blobUri, startTime)
        }
      }
      assertThat(publicationResult.customCreateTimes)
        .containsExactlyEntriesIn(expectedCustomCreateTimes)
      for (metadata in importedMetadata) {
        val sidecar = sidecarsByUri.getValue(metadata.blobUri)
        assertThat(metadata.blobTypeUrl)
          .isEqualTo("type.googleapis.com/${BlobDetails.getDescriptor().fullName}")
        assertThat(metadata.modelLine).isEqualTo(sidecar.modelLine)
        assertThat(metadata.interval).isEqualTo(sidecar.interval)
        assertThat(metadata.state).isEqualTo(ImpressionMetadata.State.ACTIVE)
        val expectedEntityKeys =
          sidecar.entityKeysList.flatMap { group ->
            group.entityIdsList.map { id ->
              entityKey {
                entityType = group.entityType
                entityId = id
              }
            }
          }
        assertThat(metadata.entityKeysList).containsExactlyElementsIn(expectedEntityKeys)
      }
      val expectedAvailability = interval {
        startTime = sidecarsByUri.values.minBy { it.interval.startTime.seconds }.interval.startTime
        endTime = sidecarsByUri.values.maxBy { it.interval.endTime.seconds }.interval.endTime
      }
      assertThat(getDataAvailabilityIntervals(edpDisplayName))
        .containsExactly(
          sidecarsByUri.values.map { it.modelLine }.distinct().single(),
          expectedAvailability,
        )
    }

    for ((edpDisplayName, eventGroupFile) in eventGroupFiles) {
      val sourceEventGroups = readEventGroups(eventGroupFile)
      val entityTypes =
        sourceEventGroups
          .map { source -> if (source.hasEntityKey()) source.entityKey.entityType else "campaign" }
          .distinct()
      assertThat(listCmmsEventGroups(edpDisplayName, entityTypes)).isEmpty()
      val mappedEventGroups = syncEventGroups(edpDisplayName, sourceEventGroups, entityTypes)
      val importedEventGroups = listCmmsEventGroups(edpDisplayName, entityTypes)
      assertThat(importedEventGroups.map { it.eventGroupReferenceId })
        .containsExactlyElementsIn(sourceEventGroups.map { it.eventGroupReferenceId })
      val expectedMappings =
        sourceEventGroups.map { source ->
          val imported =
            importedEventGroups.single { it.eventGroupReferenceId == source.eventGroupReferenceId }
          assertThat(imported.measurementConsumer).isEqualTo(source.measurementConsumer)
          assertThat(imported.dataAvailabilityInterval).isEqualTo(source.dataAvailabilityInterval)
          if (source.hasEntityKey()) {
            assertThat(imported.entityKey.entityType).isEqualTo(source.entityKey.entityType)
            assertThat(imported.entityKey.entityId).isEqualTo(source.entityKey.entityId)
          } else {
            assertThat(imported.entityKey.entityType).isEqualTo("campaign")
            assertThat(imported.entityKey.entityId).isEmpty()
          }
          assertThat(imported.mediaTypesList).containsExactly(MediaType.VIDEO)
          val campaign = imported.eventGroupMetadata.adMetadata.campaignMetadata
          assertThat(campaign.brandName)
            .isEqualTo(source.eventGroupMetadata.adMetadata.campaignMetadata.brand)
          assertThat(campaign.campaignName)
            .isEqualTo(source.eventGroupMetadata.adMetadata.campaignMetadata.campaign)
          assertThat(imported.eventGroupMetadata.entityMetadata)
            .isEqualTo(source.eventGroupMetadata.entityMetadata)
          mappedEventGroup {
            eventGroupReferenceId = source.eventGroupReferenceId
            eventGroupResource = imported.name
            if (source.hasEntityKey()) {
              entityKey = source.entityKey
            }
          }
        }
      assertThat(mappedEventGroups).containsExactlyElementsIn(expectedMappings)
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
