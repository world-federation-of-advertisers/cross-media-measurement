/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.common.service

import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.SpannerBasicReportsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMeasurementConsumersService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMeasurementsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMetricCalculationSpecsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMetricsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportScheduleIterationsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportSchedulesService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportingSetsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportsService

data class Services(
  val basicReportsService: BasicReportsGrpcKt.BasicReportsCoroutineImplBase,
  val measurementConsumersService: MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase,
  val measurementsService: MeasurementsGrpcKt.MeasurementsCoroutineImplBase,
  val metricsService: MetricsGrpcKt.MetricsCoroutineImplBase,
  val reportingSetsService: ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase,
  val reportsService: ReportsGrpcKt.ReportsCoroutineImplBase,
  val reportSchedulesService: ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase,
  val reportScheduleIterationsService:
    ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineImplBase,
  val metricCalculationSpecsService:
    MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase,
)

object DataServices {
  @JvmStatic
  fun create(
    idGenerator: IdGenerator,
    postgresClient: DatabaseClient,
    spannerClient: AsyncDatabaseClient,
  ): Services {
    return Services(
      SpannerBasicReportsService(spannerClient, postgresClient),
      PostgresMeasurementConsumersService(idGenerator, postgresClient),
      PostgresMeasurementsService(idGenerator, postgresClient),
      PostgresMetricsService(idGenerator, postgresClient),
      PostgresReportingSetsService(idGenerator, postgresClient),
      PostgresReportsService(idGenerator, postgresClient),
      PostgresReportSchedulesService(idGenerator, postgresClient),
      PostgresReportScheduleIterationsService(idGenerator, postgresClient),
      PostgresMetricCalculationSpecsService(idGenerator, postgresClient),
    )
  }
}
