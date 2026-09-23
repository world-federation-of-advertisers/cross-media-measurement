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
import com.google.protobuf.timestamp
import com.google.type.interval
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
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.Report
import org.wfanet.measurement.internal.reporting.v2.ReportKt
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.createReportRequest
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.report
import org.wfanet.measurement.internal.reporting.v2.timeIntervals
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getBasicReportByExternalId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertBasicReport
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertMeasurementConsumer
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMeasurementConsumersService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMetricCalculationSpecsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportingSetsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata as PostgresSchemata
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.internal.testing.v2.createMetricCalculationSpec
import org.wfanet.measurement.reporting.service.internal.testing.v2.createReportingSet

@RunWith(JUnit4::class)
class BasicReportExternalReportIdBackfillerTest {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.REPORTING_CHANGELOG_PATH)

  private lateinit var spannerClient: AsyncDatabaseClient
  private lateinit var postgresClient: PostgresDatabaseClient

  @Before
  fun initDatabases() = runBlocking {
    spannerClient = spannerDatabase.databaseClient
    postgresClient = postgresDatabaseProvider.createDatabase()

    PostgresMeasurementConsumersService(ID_GENERATOR, postgresClient)
      .createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )
    createReportingSet(
      CMMS_MEASUREMENT_CONSUMER_ID,
      PostgresReportingSetsService(ID_GENERATOR, postgresClient),
      EXTERNAL_REPORTING_SET_ID,
      CMMS_DATA_PROVIDER_ID,
      CMMS_EVENT_GROUP_ID,
    )
    createMetricCalculationSpec(
      CMMS_MEASUREMENT_CONSUMER_ID,
      PostgresMetricCalculationSpecsService(ID_GENERATOR, postgresClient),
      EXTERNAL_METRIC_CALCULATION_SPEC_ID,
    )
    spannerClient.readWriteTransaction().run { transaction ->
      transaction.insertMeasurementConsumer(
        measurementConsumerId = SPANNER_MEASUREMENT_CONSUMER_ID,
        measurementConsumer =
          measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID },
      )
    }
  }

  @Test
  fun `backfills matching Report ID when explicitly enabled`() =
    runBlocking<Unit> {
      createReport(EXTERNAL_BASIC_REPORT_ID, "", "")
      insertBasicReport(EXTERNAL_BASIC_REPORT_ID, "", "")

      val result = newBackfiller(dryRun = false, matchExternalBasicReportId = true).run()

      assertThat(result.updated).isEqualTo(1)
      assertThat(result.matchedByExternalBasicReportId).isEqualTo(1)
      assertThat(readBasicReport().externalReportId).isEqualTo(EXTERNAL_BASIC_REPORT_ID)
      assertThat(readBasicReport().state).isEqualTo(BasicReport.State.SUCCEEDED)
    }

  @Test
  fun `does not infer matching Report ID unless explicitly enabled`() =
    runBlocking<Unit> {
      createReport(EXTERNAL_BASIC_REPORT_ID, "", "")
      insertBasicReport(EXTERNAL_BASIC_REPORT_ID, "", "")

      val result = newBackfiller(dryRun = false, matchExternalBasicReportId = false).run()

      assertThat(result.updated).isEqualTo(0)
      assertThat(result.unresolved).isEqualTo(1)
      assertThat(readBasicReport().externalReportId).isEmpty()
    }

  @Test
  fun `backfills from Report basic_report`() =
    runBlocking<Unit> {
      val basicReportName =
        BasicReportKey(CMMS_MEASUREMENT_CONSUMER_ID, EXTERNAL_BASIC_REPORT_ID).toName()
      createReport(EXTERNAL_REPORT_ID, "", basicReportName)
      insertBasicReport(EXTERNAL_BASIC_REPORT_ID, "", "")

      val result = newBackfiller(dryRun = false, matchExternalBasicReportId = false).run()

      assertThat(result.updated).isEqualTo(1)
      assertThat(result.matchedByBasicReportName).isEqualTo(1)
      assertThat(readBasicReport().externalReportId).isEqualTo(EXTERNAL_REPORT_ID)
    }

  @Test
  fun `backfills from create Report request ID`() =
    runBlocking<Unit> {
      createReport(EXTERNAL_REPORT_ID, CREATE_REPORT_REQUEST_ID, "")
      insertBasicReport(EXTERNAL_BASIC_REPORT_ID, CREATE_REPORT_REQUEST_ID, "")

      val result = newBackfiller(dryRun = false, matchExternalBasicReportId = false).run()

      assertThat(result.updated).isEqualTo(1)
      assertThat(result.matchedByCreateReportRequestId).isEqualTo(1)
      assertThat(readBasicReport().externalReportId).isEqualTo(EXTERNAL_REPORT_ID)
    }

  @Test
  fun `skips conflicting links`() =
    runBlocking<Unit> {
      val basicReportName =
        BasicReportKey(CMMS_MEASUREMENT_CONSUMER_ID, EXTERNAL_BASIC_REPORT_ID).toName()
      createReport(EXTERNAL_BASIC_REPORT_ID, "", "")
      createReport(EXTERNAL_REPORT_ID, "", basicReportName)
      insertBasicReport(EXTERNAL_BASIC_REPORT_ID, "", "")

      val result = newBackfiller(dryRun = false, matchExternalBasicReportId = true).run()

      assertThat(result.updated).isEqualTo(0)
      assertThat(result.ambiguous).isEqualTo(1)
      assertThat(readBasicReport().externalReportId).isEmpty()
    }

  @Test
  fun `dry run writes nothing`() =
    runBlocking<Unit> {
      createReport(EXTERNAL_BASIC_REPORT_ID, "", "")
      insertBasicReport(EXTERNAL_BASIC_REPORT_ID, "", "")

      val result = newBackfiller(dryRun = true, matchExternalBasicReportId = true).run()

      assertThat(result.updated).isEqualTo(1)
      assertThat(readBasicReport().externalReportId).isEmpty()
    }

  @Test
  fun `leaves existing external Report ID unchanged`() =
    runBlocking<Unit> {
      insertBasicReport(EXTERNAL_BASIC_REPORT_ID, "", EXTERNAL_REPORT_ID)

      val result = newBackfiller(dryRun = false, matchExternalBasicReportId = true).run()

      assertThat(result.alreadyValid).isEqualTo(1)
      assertThat(result.updated).isEqualTo(0)
      assertThat(readBasicReport().externalReportId).isEqualTo(EXTERNAL_REPORT_ID)
    }

  private fun newBackfiller(
    dryRun: Boolean,
    matchExternalBasicReportId: Boolean,
  ): BasicReportExternalReportIdBackfiller {
    return BasicReportExternalReportIdBackfiller(
      spannerClient = spannerClient,
      postgresClient = postgresClient,
      dryRun = dryRun,
      createTimeAfter = null,
      cmmsMeasurementConsumerIds = setOf(CMMS_MEASUREMENT_CONSUMER_ID),
      matchExternalBasicReportId = matchExternalBasicReportId,
    )
  }

  private suspend fun createReport(
    externalReportId: String,
    createReportRequestId: String,
    basicReportName: String,
  ): Report {
    return PostgresReportsService(ID_GENERATOR, postgresClient)
      .createReport(
        createReportRequest {
          this.externalReportId = externalReportId
          requestId = createReportRequestId
          report = report {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            reportingMetricEntries[EXTERNAL_REPORTING_SET_ID] =
              ReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecReportingMetrics +=
                  ReportKt.metricCalculationSpecReportingMetrics {
                    externalMetricCalculationSpecId = EXTERNAL_METRIC_CALCULATION_SPEC_ID
                    reportingMetrics +=
                      ReportKt.reportingMetric {
                        details =
                          ReportKt.ReportingMetricKt.details {
                            metricSpec = metricSpec {
                              reach =
                                MetricSpecKt.reachParams {
                                  multipleDataProviderParams =
                                    MetricSpecKt.samplingAndPrivacyParams {
                                      privacyParams =
                                        MetricSpecKt.differentialPrivacyParams {
                                          epsilon = 1.0
                                          delta = 1e-3
                                        }
                                      vidSamplingInterval =
                                        MetricSpecKt.vidSamplingInterval {
                                          start = 0.0f
                                          width = 1.0f
                                        }
                                    }
                                }
                            }
                            timeInterval = interval {
                              startTime = timestamp { seconds = 100 }
                              endTime = timestamp { seconds = 200 }
                            }
                          }
                      }
                  }
              }
            details =
              ReportKt.details {
                basicReport = basicReportName
                timeIntervals = timeIntervals {
                  timeIntervals += interval {
                    startTime = timestamp { seconds = 100 }
                    endTime = timestamp { seconds = 200 }
                  }
                }
              }
          }
        }
      )
  }

  private suspend fun insertBasicReport(
    externalBasicReportId: String,
    createReportRequestId: String,
    externalReportId: String,
  ) {
    spannerClient.readWriteTransaction().run { transaction ->
      transaction.insertBasicReport(
        basicReportId = SPANNER_BASIC_REPORT_ID,
        measurementConsumerId = SPANNER_MEASUREMENT_CONSUMER_ID,
        basicReport =
          basicReport {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            this.externalBasicReportId = externalBasicReportId
            this.createReportRequestId = createReportRequestId
            this.externalReportId = externalReportId
          },
        state = BasicReport.State.SUCCEEDED,
        requestId = null,
      )
    }
  }

  private suspend fun readBasicReport(): BasicReport {
    return spannerClient.readOnlyTransaction().use { transaction ->
      transaction
        .getBasicReportByExternalId(CMMS_MEASUREMENT_CONSUMER_ID, EXTERNAL_BASIC_REPORT_ID)
        .basicReport
    }
  }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "measurement-consumer"
    private const val CMMS_DATA_PROVIDER_ID = "data-provider"
    private const val CMMS_EVENT_GROUP_ID = "event-group"
    private const val EXTERNAL_REPORTING_SET_ID = "reporting-set"
    private const val EXTERNAL_METRIC_CALCULATION_SPEC_ID = "metric-calculation-spec"
    private const val EXTERNAL_BASIC_REPORT_ID = "shared-report-id"
    private const val EXTERNAL_REPORT_ID = "report-id"
    private const val CREATE_REPORT_REQUEST_ID = "create-report-request-id"
    private const val SPANNER_MEASUREMENT_CONSUMER_ID = 1L
    private const val SPANNER_BASIC_REPORT_ID = 2L

    private var nextInternalId = 100L
    private var nextExternalId = 1_000L

    private val ID_GENERATOR =
      object : IdGenerator {
        override fun generateInternalId() = InternalId(nextInternalId++)

        override fun generateExternalId() = ExternalId(nextExternalId++)
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
