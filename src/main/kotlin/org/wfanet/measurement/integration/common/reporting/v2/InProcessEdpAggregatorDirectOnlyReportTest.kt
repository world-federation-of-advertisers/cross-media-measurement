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
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup as EdpaEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey as edpaEntityKey
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.copy
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

/**
 * Integration tests for single-EDP (direct protocol) EDPA report operations.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests.
 */
abstract class InProcessEdpAggregatorDirectOnlyReportTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
  secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
  accessServicesFactory: AccessServicesFactory,
  reportingDataServicesProviderRule: ProviderRule<Services>,
  duchyNames: List<String> = ALL_DUCHY_NAMES,
  hmssEnabled: Boolean,
  trusTeeEnabled: Boolean,
) :
  InProcessEdpAggregatorLifeOfAReportTest(
    kingdomDataServicesRule,
    duchyDependenciesRule,
    secureComputationDatabaseAdmin,
    accessServicesFactory,
    reportingDataServicesProviderRule,
    duchyNames,
    hmssEnabled,
    trusTeeEnabled,
    mixedIdentifierHistoryEnabled = true,
  ) {

  @Test
  fun `reports match across metadata-only legacy and entity-key blob history`() = runBlocking {
    assertMixedHistoryBlobContents()
    val eventGroups = prepareMixedHistoryEventGroups()
    val referenceIdOnlyEventGroup =
      eventGroups.single { it.eventGroupReferenceId == MIXED_HISTORY_REFERENCE_ONLY_REFERENCE_ID }
    val entityKeyOnlyEventGroup =
      eventGroups.single { it.entityKey.entityId == MIXED_HISTORY_ENTITY_ONLY_ENTITY_ID }
    val dualIdentifierEventGroup =
      eventGroups.single { it.eventGroupReferenceId == MIXED_HISTORY_DUAL_IDENTIFIER_REFERENCE_ID }

    assertThat(
        listOf(referenceIdOnlyEventGroup, entityKeyOnlyEventGroup, dualIdentifierEventGroup)
          .map { it.name }
          .toSet()
      )
      .hasSize(3)
    assertThat(referenceIdOnlyEventGroup.entityKey.entityId).isEmpty()
    assertThat(entityKeyOnlyEventGroup.eventGroupReferenceId).isEmpty()
    assertThat(dualIdentifierEventGroup.entityKey.entityId)
      .isEqualTo(MIXED_HISTORY_DUAL_IDENTIFIER_ENTITY_ID)

    val completedBasicReports =
      listOf(
        runBasicReport(referenceIdOnlyEventGroup, "mixed-history-reference-only"),
        runBasicReport(entityKeyOnlyEventGroup, "mixed-history-entity-only"),
        runBasicReport(dualIdentifierEventGroup, "mixed-history-dual-identifier"),
      )
    completedBasicReports.forEach { completedBasicReport ->
      assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
      assertStructuralResults(completedBasicReport)
      assertSingleEdpNoNoiseResults(
        completedBasicReport,
        expectedReach = EXPECTED_SINGLE_EDP_SPEC2_REACH,
        expectedImpressions = EXPECTED_SINGLE_EDP_SPEC2_IMPRESSIONS,
        expectedKPlusReach = EXPECTED_SINGLE_EDP_SPEC2_K_PLUS_REACH,
      )
      assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
    }

    val cumulativeResults =
      completedBasicReports.map { basicReport ->
        basicReport.resultGroupsList
          .single()
          .resultsList
          .single {
            it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL
          }
          .metricSet
          .reportingUnit
          .cumulative
      }
    cumulativeResults.drop(1).forEach { assertThat(it).isEqualTo(cumulativeResults.first()) }
  }

  @Test
  fun `basic report with dual and entity-key-only event groups in one requisition succeeds`() =
    runBlocking {
      val eventGroups = prepareMixedHistoryEventGroups()
      val entityKeyOnlyEventGroup =
        eventGroups.single { it.entityKey.entityId == MIXED_HISTORY_ENTITY_ONLY_ENTITY_ID }
      val dualIdentifierEventGroup =
        eventGroups.single {
          it.eventGroupReferenceId == MIXED_HISTORY_DUAL_IDENTIFIER_REFERENCE_ID
        }

      val completedBasicReport =
        runBasicReport(
          listOf(dualIdentifierEventGroup, entityKeyOnlyEventGroup),
          "mixed-history-two-event-groups",
        )

      assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
      assertStructuralResults(completedBasicReport)
      assertSingleEdpNoNoiseResults(
        completedBasicReport,
        expectedReach = EXPECTED_SINGLE_EDP_SPEC2_REACH,
        expectedImpressions = EXPECTED_TWO_ENTITY_KEY_IMPRESSIONS,
        expectedKPlusReach = EXPECTED_TWO_ENTITY_KEY_K_PLUS_REACH,
      )
      assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
    }

  @Test
  fun `campaign and exhaustive lower-level selectors return identical report results`() =
    runBlocking {
      assertThat(wholeBlobEntityTypes).containsExactly("campaign")
      val eventGroups = listReportingEventGroups()
      val campaignEventGroup =
        eventGroups.single { it.entityKey.entityId == HIERARCHY_CAMPAIGN_ENTITY_ID }
      val hierarchyEventGroups =
        eventGroups.filter {
          it.entityKey.entityId == HIERARCHY_CAMPAIGN_ENTITY_ID ||
            it.entityKey.entityId in HIERARCHY_LOWER_ENTITY_IDS.values.flatten()
        }
      assertThat(hierarchyEventGroups.map { it.name }.toSet()).hasSize(7)
      assertHierarchyBlobContents()

      val campaignReport = runBasicReport(campaignEventGroup, "hierarchy-campaign")
      assertThat(campaignReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
      assertStructuralResults(campaignReport)
      assertSingleEdpNoNoiseResults(
        campaignReport,
        expectedReach = EXPECTED_SINGLE_EDP_SPEC2_REACH,
        expectedImpressions = EXPECTED_TWO_ENTITY_KEY_IMPRESSIONS,
        expectedKPlusReach = EXPECTED_TWO_ENTITY_KEY_K_PLUS_REACH,
      )
      assertExpectedProtocolUsed(getMeasurementsForBasicReport(campaignReport.name))

      for ((entityType, entityIds) in HIERARCHY_LOWER_ENTITY_IDS) {
        assertThat(wholeBlobEntityTypes).doesNotContain(entityType)
        val lowerEventGroups = hierarchyEventGroups.filter { it.entityKey.entityType == entityType }
        assertThat(lowerEventGroups.map { it.entityKey.entityId })
          .containsExactlyElementsIn(entityIds)

        val lowerReport = runBasicReport(lowerEventGroups, "hierarchy-$entityType-all")
        assertThat(lowerReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
        assertStructuralResults(lowerReport)
        assertSingleEdpNoNoiseResults(
          lowerReport,
          expectedReach = EXPECTED_SINGLE_EDP_SPEC2_REACH,
          expectedImpressions = EXPECTED_TWO_ENTITY_KEY_IMPRESSIONS,
          expectedKPlusReach = EXPECTED_TWO_ENTITY_KEY_K_PLUS_REACH,
        )
        assertThat(lowerReport.resultGroupsList.single().resultsList.map { it.metricSet })
          .containsExactlyElementsIn(
            campaignReport.resultGroupsList.single().resultsList.map { it.metricSet }
          )
          .inOrder()
        assertExpectedProtocolUsed(getMeasurementsForBasicReport(lowerReport.name))

        // A partial selection must filter individual impressions, not accept the whole blob.
        val partialReport =
          runBasicReport(lowerEventGroups.first(), "hierarchy-$entityType-partial")
        assertThat(partialReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
        assertStructuralResults(partialReport)
        assertSingleEdpNoNoiseResults(
          partialReport,
          expectedReach = EXPECTED_SINGLE_EDP_SPEC2_REACH,
          expectedImpressions = EXPECTED_SINGLE_EDP_SPEC2_IMPRESSIONS,
          expectedKPlusReach = EXPECTED_SINGLE_EDP_SPEC2_K_PLUS_REACH,
        )
        assertExpectedProtocolUsed(getMeasurementsForBasicReport(partialReport.name))
      }
    }

  private suspend fun assertMixedHistoryBlobContents() {
    val metadata = listImpressionMetadata("edp1")
    for ((referenceId, entityId) in
      listOf(
        MIXED_HISTORY_REFERENCE_ONLY_REFERENCE_ID to MIXED_HISTORY_REFERENCE_ONLY_ENTITY_ID,
        MIXED_HISTORY_ENTITY_ONLY_REFERENCE_ID to MIXED_HISTORY_ENTITY_ONLY_ENTITY_ID,
        MIXED_HISTORY_DUAL_IDENTIFIER_REFERENCE_ID to MIXED_HISTORY_DUAL_IDENTIFIER_ENTITY_ID,
      )) {
      val blobs =
        metadata
          .filter { "/event-group-reference-id/$referenceId/" in it.blobUri }
          .map { readImpressionBlob(it) }
      val legacyBlobs = blobs.filter { it.blobDetails.entityKeysCount == 0 }
      val keyedBlobs = blobs.filter { it.blobDetails.entityKeysCount > 0 }
      assertThat(legacyBlobs).isNotEmpty()
      assertThat(keyedBlobs).isNotEmpty()

      for ((details, impressions) in legacyBlobs) {
        assertThat(details.eventGroupReferenceId).isEqualTo(referenceId)
        assertThat(impressions).isNotEmpty()
        for (impression in impressions) {
          assertThat(impression.eventGroupReferenceId).isEmpty()
          assertThat(impression.entityKeysList).isEmpty()
        }
      }
      for ((details, impressions) in keyedBlobs) {
        assertThat(details.eventGroupReferenceId).isEmpty()
        val campaignKeys = details.entityKeysList.single()
        assertThat(campaignKeys.entityType).isEqualTo("campaign")
        assertThat(campaignKeys.entityIdsList).containsExactly(entityId)
        assertThat(impressions).isNotEmpty()
        for (impression in impressions) {
          assertThat(impression.eventGroupReferenceId).isEmpty()
          val entityKey = impression.entityKeysList.single()
          assertThat(entityKey.entityType).isEqualTo("campaign")
          assertThat(entityKey.entityId).isEqualTo(entityId)
        }
      }
    }
  }

  private suspend fun assertHierarchyBlobContents() {
    val metadata =
      listImpressionMetadata("edp1").filter {
        "/event-group-reference-id/$HIERARCHY_BLOB_REFERENCE_ID/" in it.blobUri
      }
    assertThat(metadata).isNotEmpty()
    for (entry in metadata) {
      val (details, impressions) = readImpressionBlob(entry)
      // This reference ID does not match any selector; campaign uses the configured shortcut.
      assertThat(details.eventGroupReferenceId).isEqualTo(HIERARCHY_BLOB_REFERENCE_ID)
      val groupsByType = details.entityKeysList.associateBy { it.entityType }
      assertThat(groupsByType.keys)
        .containsExactlyElementsIn(HIERARCHY_LOWER_ENTITY_IDS.keys + "campaign")
      assertThat(groupsByType.getValue("campaign").entityIdsList)
        .containsExactly(HIERARCHY_CAMPAIGN_ENTITY_ID)
      for ((entityType, entityIds) in HIERARCHY_LOWER_ENTITY_IDS) {
        assertThat(groupsByType.getValue(entityType).entityIdsList)
          .containsExactlyElementsIn(entityIds)
      }
      assertThat(impressions).isNotEmpty()
      for (impression in impressions) {
        assertThat(impression.eventGroupReferenceId).isEmpty()
        val keysByType = impression.entityKeysList.associateBy { it.entityType }
        assertThat(impression.entityKeysList).hasSize(4)
        assertThat(keysByType.keys).containsExactlyElementsIn(groupsByType.keys)
        assertThat(keysByType.getValue("campaign").entityId).isEqualTo(HIERARCHY_CAMPAIGN_ENTITY_ID)
        for ((entityType, entityIds) in HIERARCHY_LOWER_ENTITY_IDS) {
          assertThat(entityIds).contains(keysByType.getValue(entityType).entityId)
        }
      }
    }
  }

  private suspend fun prepareMixedHistoryEventGroups(): List<EventGroup> {
    val eventGroups = listCmmsEventGroups("edp1", listOf("campaign"))
    val referenceIdOnlyEventGroup =
      eventGroups.single { it.eventGroupReferenceId == MIXED_HISTORY_REFERENCE_ONLY_REFERENCE_ID }
    val entityKeyOnlyEventGroup =
      eventGroups.single { it.eventGroupReferenceId == MIXED_HISTORY_ENTITY_ONLY_REFERENCE_ID }
    val dualIdentifierEventGroup =
      eventGroups.single { it.eventGroupReferenceId == MIXED_HISTORY_DUAL_IDENTIFIER_REFERENCE_ID }
    assertThat(
        listOf(referenceIdOnlyEventGroup, entityKeyOnlyEventGroup, dualIdentifierEventGroup)
          .map { it.name }
          .toSet()
      )
      .hasSize(3)
    assertThat(referenceIdOnlyEventGroup.entityKey.entityId).isEmpty()
    assertThat(entityKeyOnlyEventGroup.entityKey.entityId).isEmpty()
    assertThat(dualIdentifierEventGroup.entityKey.entityId).isEmpty()

    val sources = mixedHistoryUpgradeSources()
    val mapped =
      syncEventGroups(
        "edp1",
        listOf(
          sources.getValue(MIXED_HISTORY_ENTITY_ONLY_REFERENCE_ID),
          sources.getValue(MIXED_HISTORY_DUAL_IDENTIFIER_REFERENCE_ID),
        ),
        entityKeyTypes = listOf("campaign"),
      )
    assertThat(mapped.map { it.eventGroupResource })
      .containsExactly(entityKeyOnlyEventGroup.name, dualIdentifierEventGroup.name)
    val afterSync = listCmmsEventGroups("edp1", listOf("campaign"))
    assertThat(afterSync.map { it.name }).containsExactlyElementsIn(eventGroups.map { it.name })
    val upgradedEntityKeyOnlyEventGroup =
      afterSync.single { it.name == entityKeyOnlyEventGroup.name }
    val updatedEntityKeyOnlyEventGroup =
      updateCmmsEventGroup(upgradedEntityKeyOnlyEventGroup.copy { eventGroupReferenceId = "" })
    assertThat(updatedEntityKeyOnlyEventGroup.name).isEqualTo(entityKeyOnlyEventGroup.name)
    return listReportingEventGroups()
  }

  private suspend fun mixedHistoryUpgradeSources(): Map<String, EdpaEventGroup> {
    val sources = readEventGroups(exportEventGroups(setOf("edp1")).getValue("edp1"))
    return listOf(
        MIXED_HISTORY_REFERENCE_ONLY_REFERENCE_ID to MIXED_HISTORY_REFERENCE_ONLY_ENTITY_ID,
        MIXED_HISTORY_ENTITY_ONLY_REFERENCE_ID to MIXED_HISTORY_ENTITY_ONLY_ENTITY_ID,
        MIXED_HISTORY_DUAL_IDENTIFIER_REFERENCE_ID to MIXED_HISTORY_DUAL_IDENTIFIER_ENTITY_ID,
      )
      .associate { (referenceId, entityId) ->
        val source = sources.single { it.eventGroupReferenceId == referenceId }
        assertThat(source.hasEntityKey()).isFalse()
        referenceId to
          source.copy {
            entityKey = edpaEntityKey {
              entityType = "campaign"
              this.entityId = entityId
            }
          }
      }
  }

  @Test
  fun `sync upgrades a legacy EventGroup in place without changing report results`() = runBlocking {
    val entityTypes = listOf("campaign", CREATIVE_ID_ENTITY_TYPE, "adset-id", "ad-id")
    val beforeEventGroups = listCmmsEventGroups("edp1", entityTypes)
    val legacyEventGroup =
      beforeEventGroups.single {
        it.eventGroupReferenceId == MIXED_HISTORY_REFERENCE_ONLY_REFERENCE_ID
      }
    assertThat(legacyEventGroup.entityKey.entityId).isEmpty()
    val beforeReportingEventGroup =
      listReportingEventGroups().single { it.cmmsEventGroup == legacyEventGroup.name }
    assertThat(beforeReportingEventGroup.entityKey.entityId).isEmpty()
    val beforeReport = runBasicReport(beforeReportingEventGroup, "before-legacy-upgrade")

    val source = mixedHistoryUpgradeSources().getValue(MIXED_HISTORY_REFERENCE_ONLY_REFERENCE_ID)
    val firstSync = syncEventGroups("edp1", listOf(source), entityTypes)
    assertThat(firstSync.single().eventGroupResource).isEqualTo(legacyEventGroup.name)
    val afterEventGroups = listCmmsEventGroups("edp1", entityTypes)
    assertThat(afterEventGroups.map { it.name })
      .containsExactlyElementsIn(beforeEventGroups.map { it.name })
    val upgradedEventGroup = afterEventGroups.single { it.name == legacyEventGroup.name }
    assertThat(upgradedEventGroup.eventGroupReferenceId)
      .isEqualTo(MIXED_HISTORY_REFERENCE_ONLY_REFERENCE_ID)
    assertThat(upgradedEventGroup.entityKey.entityType).isEqualTo("campaign")
    assertThat(upgradedEventGroup.entityKey.entityId)
      .isEqualTo(MIXED_HISTORY_REFERENCE_ONLY_ENTITY_ID)

    // Repeating the migration must not create another resource.
    val secondSync = syncEventGroups("edp1", listOf(source), entityTypes)
    assertThat(secondSync.single()).isEqualTo(firstSync.single())
    assertThat(listCmmsEventGroups("edp1", entityTypes).map { it.name })
      .containsExactlyElementsIn(beforeEventGroups.map { it.name })

    val afterReportingEventGroup =
      listReportingEventGroups().single { it.cmmsEventGroup == legacyEventGroup.name }
    assertThat(afterReportingEventGroup.name).isEqualTo(beforeReportingEventGroup.name)
    assertThat(afterReportingEventGroup.entityKey.entityId)
      .isEqualTo(MIXED_HISTORY_REFERENCE_ONLY_ENTITY_ID)
    val afterReport = runBasicReport(afterReportingEventGroup, "after-legacy-upgrade")

    for (report in listOf(beforeReport, afterReport)) {
      assertThat(report.state).isEqualTo(BasicReport.State.SUCCEEDED)
      assertStructuralResults(report)
      assertSingleEdpNoNoiseResults(
        report,
        expectedReach = EXPECTED_SINGLE_EDP_SPEC2_REACH,
        expectedImpressions = EXPECTED_SINGLE_EDP_SPEC2_IMPRESSIONS,
        expectedKPlusReach = EXPECTED_SINGLE_EDP_SPEC2_K_PLUS_REACH,
      )
      assertExpectedProtocolUsed(getMeasurementsForBasicReport(report.name))
    }
    assertThat(afterReport.resultGroupsList.single().resultsList.map { it.metricSet })
      .containsExactlyElementsIn(
        beforeReport.resultGroupsList.single().resultsList.map { it.metricSet }
      )
      .inOrder()
  }

  @Test
  fun `basic report with reference-id-only event groups succeeds`() = runBlocking {
    val refIdOnlyEventGroups = getReferenceIdOnlyEventGroups()
    check(refIdOnlyEventGroups.isNotEmpty()) { "No reference-ID-only event groups found" }

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        refIdOnlyEventGroups,
        "ref-id-only-campaign",
        "ref-id-only-basicreport",
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
    assertSingleEdpNoNoiseResults(
      completedBasicReport,
      expectedReach = EXPECTED_SINGLE_EDP_SPEC2_REACH,
      expectedImpressions = EXPECTED_SINGLE_EDP_SPEC2_IMPRESSIONS,
      expectedKPlusReach = EXPECTED_SINGLE_EDP_SPEC2_K_PLUS_REACH,
    )
    val measurements = getMeasurementsForBasicReport(completedBasicReport.name)
    assertThat(
        measurements
          .map { it.measurementSpec.unpack<MeasurementSpec>().reportingMetadata.basicReport }
          .toSet()
      )
      .containsExactly(createdBasicReport.name)
    assertExpectedProtocolUsed(measurements)
  }

  @Test
  fun `basic report with single-edp creative-id event group succeeds`() = runBlocking {
    val allEventGroups = listReportingEventGroups()
    val singleCreativeIdEventGroup =
      allEventGroups.filter { it.eventGroupReferenceId == EDP1_CREATIVE_EVENT_GROUP_REF_ID }
    check(singleCreativeIdEventGroup.isNotEmpty()) {
      "No single creative-id event group found for edp1"
    }

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        singleCreativeIdEventGroup,
        "creative-id-single-edp-campaign",
        "creative-id-single-edp-basicreport",
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
    assertSingleEdpNoNoiseResults(
      completedBasicReport,
      expectedReach = EXPECTED_SINGLE_EDP_SPEC2_REACH,
      expectedImpressions = EXPECTED_SINGLE_EDP_SPEC2_IMPRESSIONS,
      expectedKPlusReach = EXPECTED_SINGLE_EDP_SPEC2_K_PLUS_REACH,
    )
    assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
  }

  @Test
  fun `basic report with mixed reference-id and entity-key event groups fails`() = runBlocking {
    val mixedEventGroups = getMixedEntityKeyEventGroups()

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        mixedEventGroups,
        "mixed-selectors-campaign",
        "mixed-selectors-basicreport",
        includeIqfFilter = false,
      )

    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(createBasicReportRequest)

    executeBasicReportsReportsJob(createdBasicReport.name)

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.FAILED)
    assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
  }

  @Test
  fun `basic report with multi-entity-key blob filtering to one entity key succeeds`() =
    runBlocking {
      val singleEntityKeyEventGroup =
        listReportingEventGroups().filter {
          it.eventGroupReferenceId == "$CREATIVE_ID_ENTITY_TYPE/$EDP1_MULTI_CREATIVE_A_ID"
        }
      check(singleEntityKeyEventGroup.isNotEmpty()) { "No multi-entity-key event group found" }

      val createBasicReportRequest =
        buildCreateBasicReportRequest(
          singleEntityKeyEventGroup,
          "multi-entity-key-campaign",
          "multi-entity-key-basicreport",
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
      assertSingleEdpNoNoiseResults(
        completedBasicReport,
        expectedReach = EXPECTED_SINGLE_EDP_SPEC2_REACH,
        expectedImpressions = EXPECTED_SINGLE_EDP_SPEC2_IMPRESSIONS,
        expectedKPlusReach = EXPECTED_SINGLE_EDP_SPEC2_K_PLUS_REACH,
      )
      assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
    }

  @Test
  fun `basic report with two same-edp entity-key event groups succeeds`() = runBlocking {
    val bothEntityKeyEventGroups =
      listReportingEventGroups().filter {
        it.eventGroupReferenceId in EDP1_MULTI_ENTITY_KEY_REF_IDS
      }
    check(bothEntityKeyEventGroups.size == 2) {
      "Expected 2 multi-entity-key event groups, got ${bothEntityKeyEventGroups.size}"
    }

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        bothEntityKeyEventGroups,
        "two-entity-key-campaign",
        "two-entity-key-basicreport",
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

    val resultGroup = completedBasicReport.resultGroupsList.single()
    val totalResult =
      resultGroup.resultsList.single {
        it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL
      }
    val reportingUnitCumulative = totalResult.metricSet.reportingUnit.cumulative

    assertWithMessage("reach")
      .that(reportingUnitCumulative.reach)
      .isEqualTo(EXPECTED_SINGLE_EDP_SPEC2_REACH)
    assertWithMessage("impressions")
      .that(reportingUnitCumulative.impressions)
      .isEqualTo(EXPECTED_TWO_ENTITY_KEY_IMPRESSIONS)
    assertWithMessage("k+ reach")
      .that(reportingUnitCumulative.kPlusReachList)
      .containsExactlyElementsIn(EXPECTED_TWO_ENTITY_KEY_K_PLUS_REACH)
      .inOrder()
    assertWithMessage("population size").that(totalResult.metricSet.populationSize).isGreaterThan(0)
    assertWithMessage("number of components")
      .that(totalResult.metricSet.componentsCount)
      .isEqualTo(1)
    assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
  }

  private suspend fun runBasicReport(eventGroup: EventGroup, id: String): BasicReport =
    runBasicReport(listOf(eventGroup), id)

  private suspend fun runBasicReport(eventGroups: List<EventGroup>, id: String): BasicReport {
    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        eventGroups,
        "$id-campaign",
        "$id-basicreport",
        includeIqfFilter = false,
      )
    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(createBasicReportRequest)

    executeBasicReportsReportsJob(createdBasicReport.name)
    executeReportProcessorJob()

    return reportingBasicReportsClient
      .withCallCredentials(credentials)
      .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })
  }
}
