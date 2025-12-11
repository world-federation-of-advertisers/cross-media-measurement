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

import com.google.protobuf.Descriptors
import kotlin.coroutines.CoroutineContext
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator as LegacyIdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersGrpcKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.SpannerBasicReportsService
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.SpannerReportResultsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMeasurementConsumersService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMeasurementsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMetricCalculationSpecsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMetricsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportScheduleIterationsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportSchedulesService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportingSetsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportsService
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping

data class Services(
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
  val basicReportsService: BasicReportsGrpcKt.BasicReportsCoroutineImplBase?,
  val impressionQualificationFiltersService:
    ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineImplBase?,
  val reportResultsService: ReportResultsGrpcKt.ReportResultsCoroutineImplBase?,
)

object DataServices {
  @JvmStatic
  fun create(
    idGenerator: IdGenerator,
    postgresClient: DatabaseClient,
    spannerClient: AsyncDatabaseClient?,
    impressionQualificationFilterMapping: ImpressionQualificationFilterMapping?,
    eventMessageDescriptor: Descriptors.Descriptor?,
    disableMetricsReuse: Boolean,
    coroutineContext: CoroutineContext,
  ): Services {
    val idGenerator = LegacyIdGeneratorAdapter(idGenerator)
    val basicReportsService: BasicReportsGrpcKt.BasicReportsCoroutineImplBase? =
      if (
        spannerClient != null &&
          impressionQualificationFilterMapping != null &&
          eventMessageDescriptor != null
      ) {
        SpannerBasicReportsService(
          spannerClient,
          postgresClient,
          impressionQualificationFilterMapping,
          coroutineContext,
        )
      } else {
        null
      }

    val impressionQualificationFiltersService:
      ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineImplBase? =
      if (impressionQualificationFilterMapping != null) {
        ImpressionQualificationFiltersService(
          impressionQualificationFilterMapping,
          coroutineContext,
        )
      } else {
        null
      }

    val reportResultsService =
      if (
        spannerClient != null &&
          impressionQualificationFilterMapping != null &&
          eventMessageDescriptor != null
      ) {
        SpannerReportResultsService(
          spannerClient,
          impressionQualificationFilterMapping,
          idGenerator,
          coroutineContext,
        )
      } else {
        null
      }

    return Services(
      measurementConsumersService =
        PostgresMeasurementConsumersService(idGenerator, postgresClient, coroutineContext),
      measurementsService =
        PostgresMeasurementsService(idGenerator, postgresClient, coroutineContext),
      metricsService = PostgresMetricsService(idGenerator, postgresClient, coroutineContext),
      reportingSetsService =
        PostgresReportingSetsService(idGenerator, postgresClient, coroutineContext),
      reportsService =
        PostgresReportsService(idGenerator, postgresClient, disableMetricsReuse, coroutineContext),
      reportSchedulesService =
        PostgresReportSchedulesService(idGenerator, postgresClient, coroutineContext),
      reportScheduleIterationsService =
        PostgresReportScheduleIterationsService(idGenerator, postgresClient, coroutineContext),
      metricCalculationSpecsService =
        PostgresMetricCalculationSpecsService(idGenerator, postgresClient, coroutineContext),
      basicReportsService = basicReportsService,
      impressionQualificationFiltersService = impressionQualificationFiltersService,
      reportResultsService = reportResultsService,
    )
  }

  private class LegacyIdGeneratorAdapter(delegate: IdGenerator) :
    IdGenerator by delegate, LegacyIdGenerator {
    override fun generateInternalId() = InternalId(generateId())

    override fun generateExternalId() = ExternalId(generateId())
  }
}
