/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.k8s

import com.google.common.truth.Truth.assertThat
import com.google.common.collect.Range
import com.google.common.truth.Truth.assertWithMessage
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import kotlinx.coroutines.runBlocking
import org.junit.Assume.assumeTrue
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.integration.common.loadSigningKey
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.reporting.ReportingUserSimulator
import org.wfanet.measurement.reporting.service.api.v2alpha.ImpressionQualificationFilterKey
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.ResultGroup

abstract class AbstractEdpAggregatorCorrectnessTest(
  private val measurementSystem: MeasurementSystem
) {

  private val mcSimulator: MeasurementConsumerSimulator
    get() = measurementSystem.mcSimulator

  protected abstract val EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS:
    ((EventGroup) -> Boolean)?
  protected abstract val EVENT_GROUP_FILTERING_LAMBDA_CROSS_PUB: ((EventGroup) -> Boolean)?

  // TODO(@marcopremier): Enable HMMS tests by adding a new EDP

  @Test
  fun `create a direct RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a direct reach and frequency measurement and verify its
      // result.
      mcSimulator.testDirectReachAndFrequency(
        "1233",
        1,
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS,
      )
    }

  @Test
  fun `create a direct reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a direct reach and frequency measurement and verify its
      // result.
      mcSimulator.testDirectReachOnly(
        "1234",
        1,
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS,
      )
    }

  @Test
  fun `create incremental direct reach only measurements in same report and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create N incremental direct reach and frequency measurements and
      // verify its result.
      mcSimulator.testDirectReachOnly(
        runId = "1235",
        numMeasurements = 3,
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS,
      )
    }

  @Test
  fun `create a impression measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create an impression measurement and verify its
      // result.
      mcSimulator.testImpression(
        "1236",
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS,
      )
    }

  @Test
  fun `create a TrusTee reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testReachOnly(
        "1237",
        ProtocolConfig.Protocol.ProtocolCase.TRUS_TEE,
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_CROSS_PUB,
      )
    }

  @Test
  fun `create a TrusTee RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testReachAndFrequency(
        "1238",
        ProtocolConfig.Protocol.ProtocolCase.TRUS_TEE,
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_CROSS_PUB,
      )
    }

  @Test
  fun `EDPA EventGroup with non-default entity_type round-trips via the CMMS public API`() =
    runBlocking {
      val response =
        measurementSystem.publicEventGroupsStub
          .withAuthenticationKey(measurementSystem.apiAuthenticationKey)
          .listEventGroups(
            listEventGroupsRequest {
              parent = measurementSystem.measurementConsumerName
              pageSize = 100
              filter =
                ListEventGroupsRequestKt.filter {
                  entityTypeIn += "campaign"
                  entityTypeIn += "creative-id"
                }
            }
          )

      val byRefId = response.eventGroupsList.associateBy { it.eventGroupReferenceId }
      val creativeId: EventGroup = byRefId.getValue(CREATIVE_ID_EVENT_GROUP_REF_ID)
      assertThat(creativeId.entityKey.entityType).isEqualTo("creative-id")
      assertThat(creativeId.entityKey.entityId).isEqualTo(CREATIVE_ID_ENTITY_ID)
      assertThat(creativeId.eventGroupMetadata.entityMetadata.fieldsMap).containsKey("placement")
    }

  @Test
  fun `direct measurement with creative-id entity-key-only event groups succeeds`() = runBlocking {
    mcSimulator.testDirectReachAndFrequency(
      "1241",
      1,
      eventGroupFilter = { it.eventGroupReferenceId == CREATIVE_ID_EVENT_GROUP_REF_ID },
      reportId = ENTITY_KEY_REPORT_ID,
    )
  }

  @Test
  fun `direct measurement with multi-entity-key blob filtering to one entity key succeeds`() =
    runBlocking {
      val response =
        measurementSystem.publicEventGroupsStub
          .withAuthenticationKey(measurementSystem.apiAuthenticationKey)
          .listEventGroups(
            listEventGroupsRequest {
              parent = measurementSystem.measurementConsumerName
              pageSize = 100
              filter = ListEventGroupsRequestKt.filter { entityTypeIn += "creative-id" }
            }
          )
      val refIds = response.eventGroupsList.map { it.eventGroupReferenceId }.toSet()
      for (expectedRefId in MULTI_CREATIVE_REF_IDS) {
        assertThat(refIds).contains(expectedRefId)
      }

      mcSimulator.testDirectReachAndFrequency(
        "1242",
        1,
        eventGroupFilter = { it.eventGroupReferenceId in MULTI_CREATIVE_REF_IDS },
        reportId = ENTITY_KEY_REPORT_ID,
      )
    }

  /** Skipped where no QA 2026 dataset is configured. */
  @Test
  fun `QA 2026 media type and impression qualification filter report succeeds`() = runBlocking {
    val reportingTestHarness = measurementSystem.reportingTestHarness
    assumeTrue(reportingTestHarness != null)

    val dates = measurementSystem.qa2026ReportDates
    val report =
      reportingTestHarness!!.createMediaTypeAndIqfBasicReport(
        measurementSystem.runId,
        measurementSystem.qa2026SingleEdpEventGroupReferenceIds,
        measurementSystem.qa2026EventGroupReferenceIds,
        dates.first(),
        dates.last(),
      )

    assertThat(report.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertReportGroups(report)
    assertReachMatchesSpecs(report, measurementSystem.qa2026ExpectedReach)
  }

  /** Checks each line item's reach against the value the synthetic specs imply. */
  private fun assertReachMatchesSpecs(
    report: BasicReport,
    expected: Map<String, Map<String, ClosedFloatingPointRange<Double>>>,
  ) {
    for (resultGroup in report.resultGroupsList) {
      val expectedByFilter = expected.getValue(resultGroup.title)
      for (result in resultGroup.resultsList) {
        val label = filterLabel(result)
        val range = expectedByFilter.getValue(label)
        assertWithMessage("${resultGroup.title}: $label reach")
          .that(reachOf(result.metricSet, resultGroup.title).toDouble())
          .isIn(Range.closed(range.start, range.endInclusive))
      }
    }
  }

  /**
   * Checks that every line item carries data and that filtered reach is bounded by unfiltered.
   *
   * `mrc` and the custom video filter each select a subset of what `ami` selects. Equality is
   * permitted: it is the correct answer whenever a filter admits every impression.
   */
  private fun assertReportGroups(report: BasicReport) {
    assertThat(report.resultGroupsList.map { it.title })
      .containsExactly(
        ReportingUserSimulator.SINGLE_EDP_GROUP_TITLE,
        ReportingUserSimulator.CROSS_PUB_GROUP_TITLE,
      )

    for (resultGroup in report.resultGroupsList) {
      assertThat(resultGroup.resultsList).hasSize(EXPECTED_FILTER_COUNT)

      val reachByFilter: Map<String, Long> =
        resultGroup.resultsList.associate { result ->
          filterLabel(result) to reachOf(result.metricSet, resultGroup.title)
        }
      for ((label, reach) in reachByFilter) {
        assertWithMessage("${resultGroup.title}: $label reach").that(reach).isGreaterThan(0L)
      }

      val amiReach = reachByFilter.getValue(ReportingUserSimulator.AMI_FILTER_ID)
      for (label in reachByFilter.keys - ReportingUserSimulator.AMI_FILTER_ID) {
        assertWithMessage("${resultGroup.title}: $label reach vs ami")
          .that(reachByFilter.getValue(label))
          .isAtMost(amiReach)
      }
    }

    // The union over every EDP reaches at least as many people as the first EDP alone.
    assertThat(amiReachOf(report, ReportingUserSimulator.CROSS_PUB_GROUP_TITLE))
      .isAtLeast(amiReachOf(report, ReportingUserSimulator.SINGLE_EDP_GROUP_TITLE))
  }

  private fun amiReachOf(report: BasicReport, groupTitle: String): Long {
    val resultGroup = report.resultGroupsList.single { it.title == groupTitle }
    val result =
      resultGroup.resultsList.single { filterLabel(it) == ReportingUserSimulator.AMI_FILTER_ID }
    return reachOf(result.metricSet, groupTitle)
  }

  /**
   * The single-EDP group requests component metrics and the cross-publisher group requests
   * reporting-unit metrics, so the reach lives in a different field for each.
   */
  private fun reachOf(metricSet: ResultGroup.MetricSet, groupTitle: String): Long =
    if (groupTitle == ReportingUserSimulator.CROSS_PUB_GROUP_TITLE) {
      metricSet.reportingUnit.nonCumulative.reach
    } else {
      metricSet.componentsList.single().value.nonCumulative.reach
    }

  private fun filterLabel(result: ResultGroup.Result): String {
    val filter = result.metadata.filter
    return if (filter.hasCustom()) {
      CUSTOM_FILTER_LABEL
    } else {
      checkNotNull(ImpressionQualificationFilterKey.fromName(filter.impressionQualificationFilter))
        .impressionQualificationFilterId
    }
  }

  interface MeasurementSystem {
    val runId: String
    val mcSimulator: MeasurementConsumerSimulator
    val publicEventGroupsStub: EventGroupsCoroutineStub
    val measurementConsumerName: String
    val apiAuthenticationKey: String

    /** Null when the environment has no QA 2026 dataset configured. */
    val reportingTestHarness: ReportingUserSimulator?
      get() = null

    /** QA 2026 EventGroup reference IDs to report on. */
    val qa2026EventGroupReferenceIds: Set<String>
      get() = emptySet()

    /** The subset of [qa2026EventGroupReferenceIds] belonging to the single-EDP result group. */
    val qa2026SingleEdpEventGroupReferenceIds: Set<String>
      get() = emptySet()

    /** Acceptable reach per result group title and impression qualification filter label. */
    val qa2026ExpectedReach: Map<String, Map<String, ClosedFloatingPointRange<Double>>>
      get() = emptyMap()

    /** QA 2026 event dates in ascending order. */
    val qa2026ReportDates: List<LocalDate>
      get() = emptyList()
  }

  companion object {
    private const val EXPECTED_FILTER_COUNT = 3
    private const val CUSTOM_FILTER_LABEL = "custom-video"

    private const val MC_ENCRYPTION_PRIVATE_KEY_NAME = "mc_enc_private.tink"
    private const val MC_CS_CERT_DER_NAME = "mc_cs_cert.der"
    private const val MC_CS_PRIVATE_KEY_DER_NAME = "mc_cs_private.der"

    // edp7's single-entity-key event group measured by the direct (single-publisher) measurements.
    // Its CMMS event_group_reference_id is derived from its `creative-id` entity key (see
    // impression_test_data_config.textproto + createEventGroups).
    const val EDP7_DIRECT_ENTITY_ID = "edpa-eg-direct-creative"
    val EDP7_DIRECT_EVENT_GROUP_REF_ID = "creative-id-$EDP7_DIRECT_ENTITY_ID"
    const val CREATIVE_ID_ENTITY_ID = "edpa-eg-creative-id-1"
    val CREATIVE_ID_EVENT_GROUP_REF_ID = "creative-id-$CREATIVE_ID_ENTITY_ID"
    const val MULTI_CREATIVE_A_ENTITY_ID = "edpa-eg-multi-creative-1"
    const val MULTI_CREATIVE_B_ENTITY_ID = "edpa-eg-multi-creative-2"
    val MULTI_CREATIVE_A_REF_ID = "creative-id-$MULTI_CREATIVE_A_ENTITY_ID"
    val MULTI_CREATIVE_B_REF_ID = "creative-id-$MULTI_CREATIVE_B_ENTITY_ID"
    val MULTI_CREATIVE_REF_IDS = setOf(MULTI_CREATIVE_A_REF_ID, MULTI_CREATIVE_B_REF_ID)
    // edpa_meta's single-entity-key event group (second publisher for the cross-publisher
    // measurement); its CMMS event_group_reference_id is derived from its `creative-id` entity key.
    const val EDPA_META_ENTITY_ID = "edpa-meta-creative"
    val EDPA_META_EVENT_GROUP_REF_ID = "creative-id-$EDPA_META_ENTITY_ID"

    // Entity-key measurements must not share a report with reference-id measurements: the
    // requisition fetcher groups requisitions by report and the results fulfiller rejects a group
    // that mixes reference-id and entity-key event groups. Route entity-key measurements to their
    // own report so each report's group is single-kind.
    const val ENTITY_KEY_REPORT_ID = "some-entity-key-report-id"

    val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 0.1
      delta = 0.000001
    }

    val MC_ENCRYPTION_PRIVATE_KEY: PrivateKeyHandle by lazy {
      loadEncryptionPrivateKey(MC_ENCRYPTION_PRIVATE_KEY_NAME)
    }

    val SECRET_FILES_PATH: Path = Paths.get("src", "main", "k8s", "testing", "secretfiles")

    val MC_SIGNING_KEY: SigningKeyHandle by lazy {
      loadSigningKey(MC_CS_CERT_DER_NAME, MC_CS_PRIVATE_KEY_DER_NAME)
    }

    val MEASUREMENT_CONSUMER_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("mc_trusted_certs.pem").toFile()
      val cert = secretFiles.resolve("mc_tls.pem").toFile()
      val key = secretFiles.resolve("mc_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    val REPORTING_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("reporting_root.pem").toFile()
      val cert = secretFiles.resolve("mc_tls.pem").toFile()
      val key = secretFiles.resolve("mc_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    val ACCESS_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("reporting_root.pem").toFile()
      val cert = secretFiles.resolve("access_tls.pem").toFile()
      val key = secretFiles.resolve("access_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    private val LOCAL_K8S_PATH: Path = Paths.get("src", "main", "k8s", "local")
    val OPEN_ID_PROVIDERS_CONFIG_JSON_FILE: File =
      LOCAL_K8S_PATH.resolve("open_id_providers_config.json").toFile()
    val OPEN_ID_PROVIDERS_TINK_FILE: File =
      SECRET_FILES_PATH.resolve("open_id_provider.tink").toFile()

    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")

    fun getRuntimePath(workspaceRelativePath: Path): Path {
      return checkNotNull(
        org.wfanet.measurement.common.getRuntimePath(WORKSPACE_PATH.resolve(workspaceRelativePath))
      )
    }
  }
}
