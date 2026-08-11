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
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
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
      // Neither database was modified.
      assertThat(readComponentSummaryReportingSetId()).isEmpty()
      assertThat(readCampaignGroupReportingSets().map { it.externalReportingSetId })
        .containsExactly(CAMPAIGN_GROUP_ID)
    }

  @Test
  fun `skips a BasicReport whose component summary has no EventGroups`() =
    runBlocking<Unit> {
      createCampaignGroup()
      insertTestBasicReport(cmmsEventGroupIds = emptyList())

      val result = newBackfiller(dryRun = false).run()

      assertThat(result.unresolvedComponents).isEqualTo(1)
      assertThat(result.skipped).isEqualTo(1)
      assertThat(result.updated).isEqualTo(0)
      assertThat(readComponentSummaryReportingSetId()).isEmpty()
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

  private suspend fun insertTestBasicReport(
    externalReportingSetId: String = "",
    cmmsEventGroupIds: List<String> = listOf(EVENT_GROUP_ID_1, EVENT_GROUP_ID_2),
  ) {
    val basicReport = internalBasicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = EXTERNAL_BASIC_REPORT_ID
      externalCampaignGroupId = CAMPAIGN_GROUP_ID
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
                      reportingUnitComponentSummary +=
                        InternalResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = CMMS_DATA_PROVIDER_ID
                          this.externalReportingSetId = externalReportingSetId
                          for (id in cmmsEventGroupIds) {
                            @Suppress("DEPRECATION") // Legacy BasicReports carry this field.
                            eventGroupSummaries +=
                              InternalResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                                .eventGroupSummary {
                                  cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                  cmmsEventGroupId = id
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
        basicReportId = BASIC_REPORT_ID,
        measurementConsumerId = MEASUREMENT_CONSUMER_ID,
        basicReport = basicReport,
        state = BasicReport.State.SUCCEEDED,
        requestId = null,
      )
    }
  }

  private suspend fun readComponentSummaryReportingSetId(): String {
    val basicReportResult =
      spannerClient.readOnlyTransaction().use { txn ->
        txn.getBasicReportByExternalId(CMMS_MEASUREMENT_CONSUMER_ID, EXTERNAL_BASIC_REPORT_ID)
      }
    return basicReportResult.basicReport.resultDetails.resultGroupsList
      .single()
      .resultsList
      .single()
      .metadata
      .reportingUnitSummary
      .reportingUnitComponentSummaryList
      .single()
      .externalReportingSetId
  }

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
    private const val EXTERNAL_BASIC_REPORT_ID = "basic-report"
    private const val MEASUREMENT_CONSUMER_ID = 1L
    private const val BASIC_REPORT_ID = 1L

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
