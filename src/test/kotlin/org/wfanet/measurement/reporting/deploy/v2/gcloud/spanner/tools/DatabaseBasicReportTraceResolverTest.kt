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
import org.wfanet.measurement.internal.reporting.v2.ReportKt
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.createReportRequest
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.report
import org.wfanet.measurement.internal.reporting.v2.timeIntervals
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertBasicReport
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertMeasurementConsumer
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMeasurementConsumersService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMetricCalculationSpecsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportingSetsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementConsumerReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata as PostgresSchemata
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.reporting.service.internal.testing.v2.createMetricCalculationSpec
import org.wfanet.measurement.reporting.service.internal.testing.v2.createReportingSet

@RunWith(JUnit4::class)
class DatabaseBasicReportTraceResolverTest {
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
    spannerClient.readWriteTransaction().run { transaction ->
      transaction.insertMeasurementConsumer(
        measurementConsumerId = SPANNER_MEASUREMENT_CONSUMER_ID,
        measurementConsumer =
          measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID },
      )
    }
  }

  @Test
  fun `resolve finds Report by request ID using Postgres MeasurementConsumer ID`() =
    runBlocking<Unit> {
      val reportingSet =
        createReportingSet(
          CMMS_MEASUREMENT_CONSUMER_ID,
          PostgresReportingSetsService(ID_GENERATOR, postgresClient),
          EXTERNAL_REPORTING_SET_ID,
          CMMS_DATA_PROVIDER_ID,
          CMMS_EVENT_GROUP_ID,
        )
      val metricCalculationSpec =
        createMetricCalculationSpec(
          CMMS_MEASUREMENT_CONSUMER_ID,
          PostgresMetricCalculationSpecsService(ID_GENERATOR, postgresClient),
          EXTERNAL_METRIC_CALCULATION_SPEC_ID,
        )
      val createdReport =
        PostgresReportsService(ID_GENERATOR, postgresClient)
          .createReport(
            createReportRequest {
              requestId = CREATE_REPORT_REQUEST_ID
              externalReportId = EXTERNAL_REPORT_ID
              report = report {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                reportingMetricEntries[reportingSet.externalReportingSetId] =
                  ReportKt.reportingMetricCalculationSpec {
                    metricCalculationSpecReportingMetrics +=
                      ReportKt.metricCalculationSpecReportingMetrics {
                        externalMetricCalculationSpecId =
                          metricCalculationSpec.externalMetricCalculationSpecId
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

      spannerClient.readWriteTransaction().run { transaction ->
        transaction.insertBasicReport(
          basicReportId = SPANNER_BASIC_REPORT_ID,
          measurementConsumerId = SPANNER_MEASUREMENT_CONSUMER_ID,
          basicReport =
            basicReport {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalBasicReportId = EXTERNAL_BASIC_REPORT_ID
              createReportRequestId = CREATE_REPORT_REQUEST_ID
            },
          state = BasicReport.State.CREATED,
          requestId = null,
        )
      }

      val readContext = postgresClient.readTransaction()
      try {
        val postgresMeasurementConsumerId =
          checkNotNull(
              MeasurementConsumerReader(readContext).getByCmmsId(CMMS_MEASUREMENT_CONSUMER_ID)
            )
            .measurementConsumerId
        assertThat(postgresMeasurementConsumerId)
          .isNotEqualTo(InternalId(SPANNER_MEASUREMENT_CONSUMER_ID))
      } finally {
        readContext.close()
      }

      val context =
        DatabaseBasicReportTraceResolver(spannerClient, postgresClient)
          .resolve(BasicReportKey(CMMS_MEASUREMENT_CONSUMER_ID, EXTERNAL_BASIC_REPORT_ID))

      assertThat(context.reportName)
        .isEqualTo(ReportKey(CMMS_MEASUREMENT_CONSUMER_ID, createdReport.externalReportId).toName())
      assertThat(context.reportResolvedByRequestId).isTrue()
      assertThat(context.unresolvedMetricRequestIds)
        .containsExactly(
          createdReport.reportingMetricEntriesMap.values
            .single()
            .metricCalculationSpecReportingMetricsList
            .single()
            .reportingMetricsList
            .single()
            .createMetricRequestId
        )
    }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "measurement-consumer"
    private const val CMMS_DATA_PROVIDER_ID = "data-provider"
    private const val CMMS_EVENT_GROUP_ID = "event-group"
    private const val EXTERNAL_REPORTING_SET_ID = "reporting-set"
    private const val EXTERNAL_METRIC_CALCULATION_SPEC_ID = "metric-calculation-spec"
    private const val EXTERNAL_BASIC_REPORT_ID = "basic-report"
    private const val EXTERNAL_REPORT_ID = "report"
    private const val CREATE_REPORT_REQUEST_ID = "create-report-request"
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
