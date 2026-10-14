/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.tools

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupKt as InternalResultGroupKt
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequestKt
import org.wfanet.measurement.internal.reporting.v2.basicReport as internalBasicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails
import org.wfanet.measurement.internal.reporting.v2.basicReportResultDetails
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.reportingSet as internalReportingSet
import org.wfanet.measurement.internal.reporting.v2.resultGroup as internalResultGroup
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getBasicReportByExternalId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertBasicReport
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertMeasurementConsumer
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMeasurementConsumersService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportingSetsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata as PostgresSchemata
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetKey
import org.wfanet.measurement.reporting.service.api.v2alpha.toBasicReport
import org.wfanet.measurement.reporting.v2alpha.BasicReport as PublicBasicReport

@RunWith(JUnit4::class)
class BasicReportReportingSetBackfillerTest {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.REPORTING_CHANGELOG_PATH)

  private lateinit var spannerClient: AsyncDatabaseClient
  private lateinit var postgresClient: PostgresDatabaseClient
  private lateinit var reportingSetsService: PostgresReportingSetsService

  @Before
  fun initClients() = runBlocking {
    spannerClient = spannerDatabase.databaseClient
    postgresClient = postgresDatabaseProvider.createDatabase()

    PostgresMeasurementConsumersService(ID_GENERATOR, postgresClient)
      .createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )
    reportingSetsService = PostgresReportingSetsService(ID_GENERATOR, postgresClient)

    // BasicReports is interleaved in MeasurementConsumers, so the parent row must exist.
    spannerClient.readWriteTransaction().run { txn ->
      txn.insertMeasurementConsumer(
        measurementConsumerId = MEASUREMENT_CONSUMER_ID,
        measurementConsumer =
          measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID },
      )
    }
  }

  @Test
  fun `reuses the existing child ReportingSet when its membership matches`() =
    runBlocking<Unit> {
      createCampaignGroup()
      val childId = createChildReportingSet(listOf(EVENT_GROUP_ID_1, EVENT_GROUP_ID_2))
      insertTestBasicReport()

      val result = newBackfiller(dryRun = false).run()

      assertThat(result.reportingSetsReused).isEqualTo(1)
      assertThat(result.reportingSetsCreated).isEqualTo(0)
      assertThat(result.updated).isEqualTo(1)
      assertThat(readComponentSummaryReportingSetId()).isEqualTo(childId)
    }

  @Test
  fun `creates a ReportingSet when no child membership matches`() =
    runBlocking<Unit> {
      createCampaignGroup()
      // Covers only one of the two EventGroups, so it must not match.
      createChildReportingSet(listOf(EVENT_GROUP_ID_1))
      insertTestBasicReport()

      val result = newBackfiller(dryRun = false).run()

      assertThat(result.reportingSetsCreated).isEqualTo(1)
      assertThat(result.reportingSetsReused).isEqualTo(0)
      assertThat(result.updated).isEqualTo(1)

      val backfilledId: String = readComponentSummaryReportingSetId()
      assertThat(backfilledId).isNotEmpty()
      val created: ReportingSet =
        readCampaignGroupReportingSets().single { it.externalReportingSetId == backfilledId }
      assertThat(created.externalCampaignGroupId).isEqualTo(CAMPAIGN_GROUP_ID)
      assertThat(created.primitive.eventGroupKeysList.map { it.cmmsEventGroupId })
        .containsExactly(EVENT_GROUP_ID_1, EVENT_GROUP_ID_2)
    }

  @Test
  fun `self-heals null CampaignGroupId on a non-empty primitive and backfills normally`() =
    runBlocking<Unit> {
      createNonCampaignGroup()
      val legacyReportingSet = readReportingSet(INVALID_CAMPAIGN_GROUP_ID)
      assertThat(legacyReportingSet.externalCampaignGroupId).isEmpty()
      assertThat(legacyReportingSet.hasPrimitive()).isTrue()
      assertThat(legacyReportingSet.primitive.eventGroupKeysCount).isGreaterThan(0)
      insertTestBasicReport(
        basicReportId = INVALID_CAMPAIGN_GROUP_BASIC_REPORT_ID,
        externalBasicReportId = INVALID_CAMPAIGN_GROUP_BASIC_REPORT_EXTERNAL_ID,
        externalCampaignGroupId = INVALID_CAMPAIGN_GROUP_ID,
        components = listOf(ComponentSpec()),
      )
      createCampaignGroup()
      val validChildId = createChildReportingSet(listOf(EVENT_GROUP_ID_1, EVENT_GROUP_ID_2))
      insertTestBasicReport(
        basicReportId = BASIC_REPORT_ID,
        externalBasicReportId = EXTERNAL_BASIC_REPORT_ID,
        externalCampaignGroupId = CAMPAIGN_GROUP_ID,
        components = listOf(ComponentSpec()),
      )

      val result = newBackfiller(dryRun = false).run()

      assertThat(result.examined).isEqualTo(2)
      assertThat(result.campaignGroupsRepaired).isEqualTo(1)
      assertThat(result.unresolvableCampaignGroupReferences).isEqualTo(0)
      assertThat(result.skipped).isEqualTo(0)
      assertThat(result.updated).isEqualTo(2)
      assertThat(result.reportingSetsCreated).isEqualTo(0)
      assertThat(result.readyToRemoveReadPathFallback).isTrue()
      assertThat(readReportingSet(INVALID_CAMPAIGN_GROUP_ID).externalCampaignGroupId)
        .isEqualTo(INVALID_CAMPAIGN_GROUP_ID)
      assertThat(
          readComponentSummaryReportingSetId(INVALID_CAMPAIGN_GROUP_BASIC_REPORT_EXTERNAL_ID)
        )
        .isEqualTo(INVALID_CAMPAIGN_GROUP_ID)
      assertThat(
          readPublicBasicReport(INVALID_CAMPAIGN_GROUP_BASIC_REPORT_EXTERNAL_ID)
            .resultGroupsList
            .single()
            .resultsList
            .single()
            .metadata
            .reportingUnitSummary
            .reportingUnitComponentSummaryList
            .single()
            .reportingSet
        )
        .isEqualTo(
          ReportingSetKey(CMMS_MEASUREMENT_CONSUMER_ID, INVALID_CAMPAIGN_GROUP_ID).toName()
        )
      assertThat(readComponentSummaryReportingSetId(EXTERNAL_BASIC_REPORT_ID))
        .isEqualTo(validChildId)

      val rerunResult = newBackfiller(dryRun = false).run()

      assertThat(rerunResult.campaignGroupsRepaired).isEqualTo(0)
      assertThat(rerunResult.alreadyValid).isEqualTo(2)
      assertThat(rerunResult.readyToRemoveReadPathFallback).isTrue()
    }

  @Test
  fun `does not self-heal a missing Campaign Group`() =
    runBlocking<Unit> {
      insertTestBasicReport(
        basicReportId = BASIC_REPORT_ID,
        externalBasicReportId = EXTERNAL_BASIC_REPORT_ID,
        externalCampaignGroupId = MISSING_CAMPAIGN_GROUP_ID,
        components = listOf(ComponentSpec()),
      )

      val result = newBackfiller(dryRun = false).run()

      assertThat(result.unresolvableCampaignGroupReferences).isEqualTo(1)
      assertThat(result.skipped).isEqualTo(1)
      assertThat(result.updated).isEqualTo(0)
      assertThat(result.readyToRemoveReadPathFallback).isFalse()
      assertThat(readComponentSummaryReportingSetId()).isEmpty()
    }

  @Test
  fun `does not self-heal a ReportingSet assigned to another Campaign Group`() =
    runBlocking<Unit> {
      createCampaignGroup()
      val childId = createChildReportingSet(listOf(EVENT_GROUP_ID_1, EVENT_GROUP_ID_2))
      insertTestBasicReport(
        basicReportId = BASIC_REPORT_ID,
        externalBasicReportId = EXTERNAL_BASIC_REPORT_ID,
        externalCampaignGroupId = childId,
        components = listOf(ComponentSpec()),
      )

      val result = newBackfiller(dryRun = false).run()

      assertThat(result.unresolvableCampaignGroupReferences).isEqualTo(1)
      assertThat(result.skipped).isEqualTo(1)
      assertThat(result.updated).isEqualTo(0)
      assertThat(result.readyToRemoveReadPathFallback).isFalse()
      assertThat(readReportingSet(childId).externalCampaignGroupId).isEqualTo(CAMPAIGN_GROUP_ID)
      assertThat(readComponentSummaryReportingSetId()).isEmpty()
    }

  @Test
  fun `does not self-heal a composite with null CampaignGroupId`() =
    runBlocking<Unit> {
      createCompositeNonCampaignGroup()
      insertTestBasicReport(
        basicReportId = BASIC_REPORT_ID,
        externalBasicReportId = EXTERNAL_BASIC_REPORT_ID,
        externalCampaignGroupId = COMPOSITE_NON_CAMPAIGN_GROUP_ID,
        components = listOf(ComponentSpec()),
      )

      val result = newBackfiller(dryRun = false).run()

      assertThat(result.campaignGroupsRepaired).isEqualTo(0)
      assertThat(result.unresolvableCampaignGroupReferences).isEqualTo(1)
      assertThat(result.readyToRemoveReadPathFallback).isFalse()
      val reportingSet = readReportingSet(COMPOSITE_NON_CAMPAIGN_GROUP_ID)
      assertThat(reportingSet.externalCampaignGroupId).isEmpty()
      assertThat(reportingSet.hasComposite()).isTrue()
      assertThat(readComponentSummaryReportingSetId()).isEmpty()
    }

  @Test
  fun `does not self-heal an empty primitive with null CampaignGroupId`() =
    runBlocking<Unit> {
      createEmptyNonCampaignGroup()
      insertTestBasicReport(
        basicReportId = BASIC_REPORT_ID,
        externalBasicReportId = EXTERNAL_BASIC_REPORT_ID,
        externalCampaignGroupId = EMPTY_NON_CAMPAIGN_GROUP_ID,
        components = listOf(ComponentSpec()),
      )

      val result = newBackfiller(dryRun = false).run()

      assertThat(result.campaignGroupsRepaired).isEqualTo(0)
      assertThat(result.unresolvableCampaignGroupReferences).isEqualTo(1)
      assertThat(result.readyToRemoveReadPathFallback).isFalse()
      assertThat(readReportingSet(EMPTY_NON_CAMPAIGN_GROUP_ID).externalCampaignGroupId).isEmpty()
      assertThat(readComponentSummaryReportingSetId()).isEmpty()
    }

  @Test
  fun `leaves a BasicReport that already has external_reporting_set_id untouched`() =
    runBlocking<Unit> {
      createCampaignGroup()
      val childId = createChildReportingSet(listOf(EVENT_GROUP_ID_1, EVENT_GROUP_ID_2))
      insertTestBasicReport(externalReportingSetId = childId)

      val result = newBackfiller(dryRun = false).run()

      assertThat(result.alreadyValid).isEqualTo(1)
      assertThat(result.updated).isEqualTo(0)
      assertThat(result.reportingSetsCreated).isEqualTo(0)
      assertThat(readComponentSummaryReportingSetId()).isEqualTo(childId)
    }

  @Test
  fun `dry run writes nothing`() =
    runBlocking<Unit> {
      createCampaignGroup()
      // No matching child, so a non-dry run would create one.
      insertTestBasicReport()

      val result = newBackfiller(dryRun = true).run()

      assertThat(result.updated).isEqualTo(1)
      assertThat(result.reportingSetsCreated).isEqualTo(1)
      assertThat(result.readyToRemoveReadPathFallback).isFalse()
      // Neither database was modified.
      assertThat(readComponentSummaryReportingSetId()).isEmpty()
      assertThat(readCampaignGroupReportingSets().map { it.externalReportingSetId })
        .containsExactly(CAMPAIGN_GROUP_ID)
    }

  @Test
  fun `dry run simulates repairing a legacy Campaign Group without writing`() =
    runBlocking<Unit> {
      createNonCampaignGroup()
      insertTestBasicReport(
        basicReportId = BASIC_REPORT_ID,
        externalBasicReportId = EXTERNAL_BASIC_REPORT_ID,
        externalCampaignGroupId = INVALID_CAMPAIGN_GROUP_ID,
        components = listOf(ComponentSpec()),
      )

      val result = newBackfiller(dryRun = true).run()

      assertThat(result.campaignGroupsRepaired).isEqualTo(1)
      assertThat(result.updated).isEqualTo(1)
      assertThat(result.reportingSetsCreated).isEqualTo(0)
      assertThat(result.readyToRemoveReadPathFallback).isFalse()
      assertThat(readReportingSet(INVALID_CAMPAIGN_GROUP_ID).externalCampaignGroupId).isEmpty()
      assertThat(readComponentSummaryReportingSetId()).isEmpty()
    }

  @Test
  fun `reports not ready when a component summary has no EventGroups`() =
    runBlocking<Unit> {
      createCampaignGroup()
      insertTestBasicReport(cmmsEventGroupIds = emptyList())

      val result = newBackfiller(dryRun = false).run()

      assertThat(result.unresolvedComponents).isEqualTo(1)
      assertThat(result.skipped).isEqualTo(1)
      assertThat(result.updated).isEqualTo(0)
      assertThat(result.readyToRemoveReadPathFallback).isFalse()
      assertThat(readComponentSummaryReportingSetId()).isEmpty()
    }

  @Test
  fun `mints nothing when a sibling component summary in the same BasicReport is unresolvable`() =
    runBlocking<Unit> {
      createCampaignGroup()
      insertTestBasicReport(
        listOf(
          // Resolvable, and no existing child matches, so this would be minted on its own.
          ComponentSpec(cmmsEventGroupIds = listOf(EVENT_GROUP_ID_1, EVENT_GROUP_ID_2)),
          // Unresolvable: carries no EventGroups at all.
          ComponentSpec(
            cmmsDataProviderId = OTHER_CMMS_DATA_PROVIDER_ID,
            cmmsEventGroupIds = emptyList(),
          ),
        )
      )

      val result = newBackfiller(dryRun = false).run()

      assertThat(result.reportingSetsCreated).isEqualTo(0)
      assertThat(result.readyToRemoveReadPathFallback).isFalse()
      // The resolvable component must not have left an orphaned ReportingSet behind.
      assertThat(readCampaignGroupReportingSets().map { it.externalReportingSetId })
        .containsExactly(CAMPAIGN_GROUP_ID)
      assertThat(readComponentSummaryReportingSetIds()).containsExactly("", "")
    }

  @Test
  fun `leaves an already valid component summary untouched while backfilling its sibling`() =
    runBlocking<Unit> {
      createCampaignGroup()
      val childId = createChildReportingSet(listOf(EVENT_GROUP_ID_1, EVENT_GROUP_ID_2))
      insertTestBasicReport(
        listOf(
          ComponentSpec(externalReportingSetId = childId),
          ComponentSpec(
            cmmsDataProviderId = OTHER_CMMS_DATA_PROVIDER_ID,
            cmmsEventGroupIds = listOf(OTHER_EVENT_GROUP_ID),
          ),
        )
      )

      val result = newBackfiller(dryRun = false).run()

      assertThat(result.updated).isEqualTo(1)
      assertThat(result.reportingSetsCreated).isEqualTo(1)
      val reportingSetIds: List<String> = readComponentSummaryReportingSetIds()
      assertThat(reportingSetIds).hasSize(2)
      assertThat(reportingSetIds[0]).isEqualTo(childId)
      assertThat(reportingSetIds[1]).isNotEmpty()
      assertThat(reportingSetIds[1]).isNotEqualTo(childId)
    }

  @Test
  fun `backfilled BasicReport exposes reporting_set through the public read path`() =
    runBlocking<Unit> {
      createCampaignGroup()
      val childId = createChildReportingSet(listOf(EVENT_GROUP_ID_1, EVENT_GROUP_ID_2))
      insertTestBasicReport()

      assertThat(newBackfiller(dryRun = false).run().updated).isEqualTo(1)

      // The read path silently omits reporting_set for an unbackfilled component summary, so this
      // is what actually proves the BasicReport is serveable again.
      val componentSummary =
        readPublicBasicReport()
          .resultGroupsList
          .single()
          .resultsList
          .single()
          .metadata
          .reportingUnitSummary
          .reportingUnitComponentSummaryList
          .single()
      assertThat(componentSummary.reportingSet)
        .isEqualTo(ReportingSetKey(CMMS_MEASUREMENT_CONSUMER_ID, childId).toName())
    }

  private fun newBackfiller(dryRun: Boolean) =
    BasicReportReportingSetBackfiller(spannerClient, postgresClient, ID_GENERATOR, dryRun)

  private suspend fun createCampaignGroup() {
    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        externalReportingSetId = CAMPAIGN_GROUP_ID
        reportingSet = internalReportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalCampaignGroupId = CAMPAIGN_GROUP_ID
          primitive =
            ReportingSetKt.primitive {
              eventGroupKeys += eventGroupKey(EVENT_GROUP_ID_1)
              eventGroupKeys += eventGroupKey(EVENT_GROUP_ID_2)
              eventGroupKeys += eventGroupKey(OTHER_EVENT_GROUP_ID, OTHER_CMMS_DATA_PROVIDER_ID)
            }
        }
      }
    )
  }

  private suspend fun createNonCampaignGroup() {
    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        externalReportingSetId = INVALID_CAMPAIGN_GROUP_ID
        reportingSet = internalReportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          primitive =
            ReportingSetKt.primitive {
              eventGroupKeys += eventGroupKey(EVENT_GROUP_ID_1)
              eventGroupKeys += eventGroupKey(EVENT_GROUP_ID_2)
            }
        }
      }
    )
  }

  private suspend fun createCompositeNonCampaignGroup() {
    createNonCampaignGroup()
    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        externalReportingSetId = COMPOSITE_NON_CAMPAIGN_GROUP_ID
        reportingSet = internalReportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          composite =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.UNION
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  externalReportingSetId = INVALID_CAMPAIGN_GROUP_ID
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  externalReportingSetId = INVALID_CAMPAIGN_GROUP_ID
                }
            }
          weightedSubsetUnions +=
            ReportingSetKt.weightedSubsetUnion {
              weight = 1
              binaryRepresentation = 1
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = INVALID_CAMPAIGN_GROUP_ID
                }
            }
        }
      }
    )
  }

  private suspend fun createEmptyNonCampaignGroup() {
    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        externalReportingSetId = EMPTY_NON_CAMPAIGN_GROUP_ID
        reportingSet = internalReportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          primitive = ReportingSetKt.primitive { eventGroupKeys += eventGroupKey(EVENT_GROUP_ID_1) }
        }
      }
    )

    val transactionContext = postgresClient.readWriteTransaction()
    try {
      transactionContext.executeStatement(
        boundStatement(
          """
          DELETE FROM ReportingSetEventGroups
          WHERE (MeasurementConsumerId, ReportingSetId) =
            (
              SELECT MeasurementConsumerId, ReportingSetId
              FROM ReportingSets
                JOIN MeasurementConsumers USING (MeasurementConsumerId)
              WHERE CmmsMeasurementConsumerId = $1
                AND ExternalReportingSetId = $2
            )
          """
            .trimIndent()
        ) {
          bind("$1", CMMS_MEASUREMENT_CONSUMER_ID)
          bind("$2", EMPTY_NON_CAMPAIGN_GROUP_ID)
        }
      )
      transactionContext.commit()
    } finally {
      transactionContext.close()
    }
  }

  private suspend fun createChildReportingSet(cmmsEventGroupIds: List<String>): String {
    val externalId = "child-${cmmsEventGroupIds.joinToString("-")}"
    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        externalReportingSetId = externalId
        reportingSet = internalReportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalCampaignGroupId = CAMPAIGN_GROUP_ID
          primitive =
            ReportingSetKt.primitive {
              for (id in cmmsEventGroupIds) {
                eventGroupKeys += eventGroupKey(id)
              }
            }
        }
      }
    )
    return externalId
  }

  private fun eventGroupKey(
    cmmsEventGroupId: String,
    cmmsDataProviderId: String = CMMS_DATA_PROVIDER_ID,
  ) =
    ReportingSetKt.PrimitiveKt.eventGroupKey {
      this.cmmsDataProviderId = cmmsDataProviderId
      this.cmmsEventGroupId = cmmsEventGroupId
    }

  /** One `ReportingUnitComponentSummary` to seed into the test `BasicReport`. */
  private data class ComponentSpec(
    val cmmsDataProviderId: String = CMMS_DATA_PROVIDER_ID,
    val cmmsEventGroupIds: List<String> = listOf(EVENT_GROUP_ID_1, EVENT_GROUP_ID_2),
    val externalReportingSetId: String = "",
  )

  private suspend fun insertTestBasicReport(
    externalReportingSetId: String = "",
    cmmsEventGroupIds: List<String> = listOf(EVENT_GROUP_ID_1, EVENT_GROUP_ID_2),
  ) =
    insertTestBasicReport(
      listOf(
        ComponentSpec(
          cmmsEventGroupIds = cmmsEventGroupIds,
          externalReportingSetId = externalReportingSetId,
        )
      )
    )

  /** Inserts a `BasicReport` with one `ReportingUnitComponentSummary` per entry in [components]. */
  private suspend fun insertTestBasicReport(components: List<ComponentSpec>) =
    insertTestBasicReport(
      basicReportId = BASIC_REPORT_ID,
      externalBasicReportId = EXTERNAL_BASIC_REPORT_ID,
      externalCampaignGroupId = CAMPAIGN_GROUP_ID,
      components = components,
    )

  private suspend fun insertTestBasicReport(
    basicReportId: Long,
    externalBasicReportId: String,
    externalCampaignGroupId: String,
    components: List<ComponentSpec>,
  ) {
    val basicReport = internalBasicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      this.externalBasicReportId = externalBasicReportId
      this.externalCampaignGroupId = externalCampaignGroupId
      details = basicReportDetails {}
      resultDetails = basicReportResultDetails {
        resultGroups += internalResultGroup {
          title = "title"
          results +=
            InternalResultGroupKt.result {
              metadata =
                InternalResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    InternalResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      for (component in components) {
                        reportingUnitComponentSummary +=
                          InternalResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                            cmmsDataProviderId = component.cmmsDataProviderId
                            externalReportingSetId = component.externalReportingSetId
                            for (id in component.cmmsEventGroupIds) {
                              @Suppress("DEPRECATION") // Legacy BasicReports carry this field.
                              eventGroupSummaries +=
                                InternalResultGroupKt.MetricMetadataKt
                                  .ReportingUnitComponentSummaryKt
                                  .eventGroupSummary {
                                    cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                    cmmsEventGroupId = id
                                  }
                            }
                          }
                      }
                    }
                }
              metricSet = InternalResultGroupKt.metricSet {}
            }
        }
      }
    }

    spannerClient.readWriteTransaction().run { txn ->
      txn.insertBasicReport(
        basicReportId = basicReportId,
        measurementConsumerId = MEASUREMENT_CONSUMER_ID,
        basicReport = basicReport,
        state = BasicReport.State.SUCCEEDED,
        requestId = null,
      )
    }
  }

  private suspend fun readComponentSummaryReportingSetId(): String =
    readComponentSummaryReportingSetId(EXTERNAL_BASIC_REPORT_ID)

  private suspend fun readComponentSummaryReportingSetId(externalBasicReportId: String): String =
    readComponentSummaryReportingSetIds(externalBasicReportId).single()

  /** The `external_reporting_set_id` of every component summary, in order. */
  private suspend fun readComponentSummaryReportingSetIds(): List<String> =
    readComponentSummaryReportingSetIds(EXTERNAL_BASIC_REPORT_ID)

  private suspend fun readComponentSummaryReportingSetIds(
    externalBasicReportId: String
  ): List<String> =
    readInternalBasicReport(externalBasicReportId)
      .resultDetails
      .resultGroupsList
      .single()
      .resultsList
      .single()
      .metadata
      .reportingUnitSummary
      .reportingUnitComponentSummaryList
      .map { it.externalReportingSetId }

  private suspend fun readInternalBasicReport(): BasicReport =
    readInternalBasicReport(EXTERNAL_BASIC_REPORT_ID)

  private suspend fun readInternalBasicReport(externalBasicReportId: String): BasicReport =
    spannerClient
      .readOnlyTransaction()
      .use { txn ->
        txn.getBasicReportByExternalId(CMMS_MEASUREMENT_CONSUMER_ID, externalBasicReportId)
      }
      .basicReport

  private suspend fun readReportingSet(externalReportingSetId: String): ReportingSet {
    val readContext = postgresClient.singleUse()
    return try {
      ReportingSetReader(readContext)
        .batchGetReportingSets(
          batchGetReportingSetsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportingSetIds += externalReportingSetId
          }
        )
        .single()
        .reportingSet
    } finally {
      readContext.close()
    }
  }

  /** Reads the stored `BasicReport` back through the public read path. */
  private suspend fun readPublicBasicReport(): PublicBasicReport =
    readPublicBasicReport(EXTERNAL_BASIC_REPORT_ID)

  private suspend fun readPublicBasicReport(externalBasicReportId: String): PublicBasicReport =
    readInternalBasicReport(externalBasicReportId)
      .toBasicReport(populateDeprecatedReportingUnitEventGroupSummaries = false)

  /** All ReportingSets under the Campaign Group, including the Campaign Group itself. */
  private suspend fun readCampaignGroupReportingSets(): List<ReportingSet> {
    val readContext = postgresClient.singleUse()
    return try {
      ReportingSetReader(readContext)
        .readReportingSets(
          streamReportingSetsRequest {
            filter =
              StreamReportingSetsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalCampaignGroupId = CAMPAIGN_GROUP_ID
              }
            limit = Int.MAX_VALUE
          }
        )
        .map { it.reportingSet }
        .toList()
    } finally {
      readContext.close()
    }
  }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "mc"
    private const val CMMS_DATA_PROVIDER_ID = "dp"
    private const val EVENT_GROUP_ID_1 = "eg1"
    private const val EVENT_GROUP_ID_2 = "eg2"
    private const val OTHER_CMMS_DATA_PROVIDER_ID = "dp2"
    private const val OTHER_EVENT_GROUP_ID = "eg3"
    private const val CAMPAIGN_GROUP_ID = "campaign-group"
    private const val INVALID_CAMPAIGN_GROUP_ID = "invalid-campaign-group"
    private const val COMPOSITE_NON_CAMPAIGN_GROUP_ID = "composite-non-campaign-group"
    private const val EMPTY_NON_CAMPAIGN_GROUP_ID = "empty-non-campaign-group"
    private const val MISSING_CAMPAIGN_GROUP_ID = "missing-campaign-group"
    private const val EXTERNAL_BASIC_REPORT_ID = "basic-report"
    private const val INVALID_CAMPAIGN_GROUP_BASIC_REPORT_EXTERNAL_ID =
      "invalid-campaign-group-basic-report"
    private const val MEASUREMENT_CONSUMER_ID = 1L
    private const val BASIC_REPORT_ID = 1L
    private const val INVALID_CAMPAIGN_GROUP_BASIC_REPORT_ID = 2L

    private var idCounter = 0L

    private val ID_GENERATOR =
      object : IdGenerator {
        override fun generateInternalId() = InternalId(++idCounter)

        override fun generateExternalId() = ExternalId(++idCounter)
      }

    @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    @JvmStatic
    val postgresDatabaseProvider =
      PostgresDatabaseProviderRule(PostgresSchemata.REPORTING_CHANGELOG_PATH)

    @get:ClassRule
    @JvmStatic
    val ruleChain: TestRule = chainRulesSequentially(spannerEmulator, postgresDatabaseProvider)
  }
}
