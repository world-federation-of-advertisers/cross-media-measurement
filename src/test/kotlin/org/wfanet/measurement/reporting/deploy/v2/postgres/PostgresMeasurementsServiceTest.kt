/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.postgres

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import com.google.type.interval
import java.util.UUID
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequestKt
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementResultsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementResultsRequestKt
import org.wfanet.measurement.internal.reporting.v2.DeterministicCountDistinct
import org.wfanet.measurement.internal.reporting.v2.MeasurementKt
import org.wfanet.measurement.internal.reporting.v2.Metric
import org.wfanet.measurement.internal.reporting.v2.MetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.NoiseMechanism
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementResultsRequest
import org.wfanet.measurement.internal.reporting.v2.createMetricRequest
import org.wfanet.measurement.internal.reporting.v2.measurement
import org.wfanet.measurement.internal.reporting.v2.metric
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata
import org.wfanet.measurement.reporting.service.internal.testing.v2.MeasurementsServiceTest
import org.wfanet.measurement.reporting.service.internal.testing.v2.createMeasurementConsumer
import org.wfanet.measurement.reporting.service.internal.testing.v2.createReportingSet

@RunWith(JUnit4::class)
class PostgresMeasurementsServiceTest : MeasurementsServiceTest<PostgresMeasurementsService>() {
  override fun newServices(idGenerator: IdGenerator): Services<PostgresMeasurementsService> {
    val client: PostgresDatabaseClient = databaseProvider.createDatabase()
    return Services(
      PostgresMeasurementsService(idGenerator, client),
      PostgresMetricsService(idGenerator, client),
      PostgresReportingSetsService(idGenerator, client),
      PostgresMeasurementConsumersService(idGenerator, client),
    )
  }

  @Test
  fun `batchSetMeasurementResults updates stranded Metric state`(): Unit = runBlocking {
    val client: PostgresDatabaseClient = databaseProvider.createDatabase()
    val measurementsService = PostgresMeasurementsService(idGenerator, client)
    val metricsService = PostgresMetricsService(idGenerator, client)
    val reportingSetsService = PostgresReportingSetsService(idGenerator, client)
    val measurementConsumersService = PostgresMeasurementConsumersService(idGenerator, client)

    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet: ReportingSet =
      createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createdMetric =
      metricsService.createMetric(
        createMetricRequest {
          metric = buildReachMetric(createdReportingSet.externalReportingSetId)
          externalMetricId = EXTERNAL_METRIC_ID
        }
      )
    measurementsService.batchSetCmmsMeasurementIds(
      batchSetCmmsMeasurementIdsRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementIds +=
          BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
            cmmsCreateMeasurementRequestId =
              createdMetric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = CMMS_MEASUREMENT_ID
          }
      }
    )

    val request: BatchSetMeasurementResultsRequest = batchSetMeasurementResultsRequest {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      measurementResults +=
        BatchSetMeasurementResultsRequestKt.measurementResult {
          cmmsMeasurementId = CMMS_MEASUREMENT_ID
          results +=
            MeasurementKt.result {
              reach =
                MeasurementKt.ResultKt.reach {
                  value = 1
                  noiseMechanism = NoiseMechanism.GEOMETRIC
                  deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
                }
            }
        }
    }
    measurementsService.batchSetMeasurementResults(request)

    // Put the Metric into the state where it is RUNNING while its Measurement is SUCCEEDED.
    client.setMetricState(EXTERNAL_METRIC_ID, Metric.State.RUNNING)

    measurementsService.batchSetMeasurementResults(request)

    val metrics =
      metricsService.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += EXTERNAL_METRIC_ID
        }
      )
    assertThat(metrics.metricsList.single().state).isEqualTo(Metric.State.SUCCEEDED)
  }

  private suspend fun PostgresDatabaseClient.setMetricState(
    externalMetricId: String,
    state: Metric.State,
  ) {
    val transactionContext = readWriteTransaction()
    try {
      transactionContext.executeStatement(
        boundStatement("UPDATE Metrics SET State = $1 WHERE ExternalMetricId = $2") {
          bind("$1", state)
          bind("$2", externalMetricId)
        }
      )
      transactionContext.commit()
    } finally {
      transactionContext.close()
    }
  }

  private fun buildReachMetric(externalReportingSetId: String): Metric {
    return metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      this.externalReportingSetId = externalReportingSetId
      timeInterval = METRIC_TIME_INTERVAL
      metricSpec = metricSpec {
        reach =
          MetricSpecKt.reachParams {
            multipleDataProviderParams =
              MetricSpecKt.samplingAndPrivacyParams {
                privacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsCreateMeasurementRequestId = UUID.randomUUID().toString()
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = METRIC_TIME_INTERVAL
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                this.externalReportingSetId = externalReportingSetId
              }
            details = MeasurementKt.details { dataProviderCount = 3 }
          }
        }
    }
  }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"
    private const val CMMS_MEASUREMENT_ID = "1234"
    private const val EXTERNAL_METRIC_ID = "external-metric-id"

    private val METRIC_TIME_INTERVAL = interval {
      startTime = timestamp { seconds = 10 }
      endTime = timestamp { seconds = 100 }
    }

    @get:ClassRule
    @JvmStatic
    val databaseProvider = PostgresDatabaseProviderRule(Schemata.REPORTING_CHANGELOG_PATH)
  }
}
