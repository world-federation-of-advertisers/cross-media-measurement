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
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.v2.Metric
import org.wfanet.measurement.internal.reporting.v2.MetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.batchCreateMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.createMetricRequest
import org.wfanet.measurement.internal.reporting.v2.measurement
import org.wfanet.measurement.internal.reporting.v2.metric
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata
import org.wfanet.measurement.reporting.service.internal.testing.v2.MetricsServiceTest
import org.wfanet.measurement.reporting.service.internal.testing.v2.createMeasurementConsumer
import org.wfanet.measurement.reporting.service.internal.testing.v2.createReportingSet

@RunWith(JUnit4::class)
class PostgresMetricsServiceTest : MetricsServiceTest<PostgresMetricsService>() {
  override fun newServices(idGenerator: IdGenerator): Services<PostgresMetricsService> {
    val client: PostgresDatabaseClient = databaseProvider.createDatabase()
    return Services(
      PostgresMetricsService(idGenerator, client),
      PostgresReportingSetsService(idGenerator, client),
      PostgresMeasurementConsumersService(idGenerator, client),
      PostgresMeasurementsService(idGenerator, client),
    )
  }

  @Test
  fun `batchCreateMetrics stores high fanout filters beyond PostgreSQL bind limit`(): Unit =
    runBlocking {
      val services = newServices(idGenerator)
      val consumer = "scale-consumer"
      createMeasurementConsumer(consumer, services.measurementConsumersService)
      val reportingSet = createReportingSet(consumer, services.reportingSetsService)
      val filters = (1..6).map { "filter$it" }
      val timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      val template = metric {
        cmmsMeasurementConsumerId = consumer
        externalReportingSetId = reportingSet.externalReportingSetId
        this.timeInterval = timeInterval
        metricSpec = metricSpec {
          reach =
            MetricSpecKt.reachParams {
              multipleDataProviderParams =
                MetricSpecKt.samplingAndPrivacyParams {
                  privacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 0.01
                    }
                  vidSamplingInterval =
                    MetricSpecKt.vidSamplingInterval {
                      start = 0.1f
                      width = 0.5f
                    }
                }
            }
        }
        for (index in 1..3) {
          weightedMeasurements +=
            MetricKt.weightedMeasurement {
              weight = 1
              binaryRepresentation = index
              measurement = measurement {
                cmmsMeasurementConsumerId = consumer
                this.timeInterval = timeInterval
                primitiveReportingSetBases +=
                  ReportingSetKt.primitiveReportingSetBasis {
                    externalReportingSetId = reportingSet.externalReportingSetId
                    this.filters += filters
                  }
              }
            }
        }
      }
      // 1,000 metrics * 3 measurements * 6 filters * 4 bindings = 72,000 SQL parameters.
      val request = batchCreateMetricsRequest {
        cmmsMeasurementConsumerId = consumer
        requests +=
          (0 until 1000).map { index ->
            createMetricRequest {
              this.metric = template
              externalMetricId = "scale-metric-$index"
              requestId = UUID.nameUUIDFromBytes("scale-metric-$index".toByteArray()).toString()
            }
          }
      }
      val created = services.metricsService.batchCreateMetrics(request).metricsList
      assertThat(created).hasSize(1000)
      assertThat(created.map { it.externalMetricId }.toSet()).hasSize(1000)
      val measurementIds =
        created
          .flatMap {
            it.weightedMeasurementsList.map { weighted ->
              weighted.measurement.cmmsCreateMeasurementRequestId
            }
          }
          .toSet()
      assertThat(measurementIds).hasSize(3000)
      val retrieved =
        services.metricsService
          .batchGetMetrics(
            batchGetMetricsRequest {
              cmmsMeasurementConsumerId = consumer
              externalMetricIds += created.map { it.externalMetricId }
            }
          )
          .metricsList
      assertThat(retrieved.map { it.externalMetricId }.toSet())
        .isEqualTo(created.map { it.externalMetricId }.toSet())
      for (value in retrieved) {
        assertThat(value.state).isEqualTo(Metric.State.RUNNING)
        assertThat(value.weightedMeasurementsList).hasSize(3)
        for (weighted in value.weightedMeasurementsList) {
          assertThat(weighted.measurement.primitiveReportingSetBasesList).hasSize(1)
          assertThat(weighted.measurement.primitiveReportingSetBasesList.single().filtersList)
            .containsExactlyElementsIn(filters)
        }
      }
      val replayed = services.metricsService.batchCreateMetrics(request).metricsList
      assertThat(replayed.map { it.externalMetricId }.toSet())
        .isEqualTo(created.map { it.externalMetricId }.toSet())
      assertThat(
          replayed
            .flatMap {
              it.weightedMeasurementsList.map { weighted ->
                weighted.measurement.cmmsCreateMeasurementRequestId
              }
            }
            .toSet()
        )
        .isEqualTo(measurementIds)
    }

  companion object {
    @get:ClassRule
    @JvmStatic
    val databaseProvider = PostgresDatabaseProviderRule(Schemata.REPORTING_CHANGELOG_PATH)
  }
}
