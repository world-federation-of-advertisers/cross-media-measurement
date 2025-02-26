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

package org.wfanet.measurement.reporting.deploy.v2.common.server

import io.grpc.BindableService
import kotlin.reflect.full.declaredMemberProperties
import kotlinx.coroutines.runInterruptible
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineImplBase
import picocli.CommandLine

abstract class InternalReportingServer : Runnable {
  data class Services(
    val basicReportsService: BasicReportsCoroutineImplBase,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val measurementsService: MeasurementsCoroutineImplBase,
    val metricsService: MetricsCoroutineImplBase,
    val reportingSetsService: ReportingSetsCoroutineImplBase,
    val reportsService: ReportsCoroutineImplBase,
    val reportSchedulesService: ReportSchedulesCoroutineImplBase,
    val reportScheduleIterationsService: ReportScheduleIterationsCoroutineImplBase,
    val metricCalculationSpecsService: MetricCalculationSpecsCoroutineImplBase,
  )

  @CommandLine.Mixin private lateinit var serverFlags: CommonServer.Flags

  protected suspend fun run(services: Services) {
    val server = CommonServer.fromFlags(serverFlags, this::class.simpleName!!, services.toList())

    runInterruptible { server.start().blockUntilShutdown() }
  }

  companion object {
    fun Services.toList(): List<BindableService> {
      return Services::class.declaredMemberProperties.map { it.get(this) as BindableService }
    }
  }
}
