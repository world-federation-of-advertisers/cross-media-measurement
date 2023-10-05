package org.wfanet.measurement.reporting.service.internal.testing.v2

import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.reportingSet

suspend fun createReportingSet(
  cmmsMeasurementConsumerId: String,
  reportingSetsService: ReportingSetsCoroutineImplBase,
  externalReportingSetId: String = "external-reporting-set-id"
): ReportingSet {
  val reportingSet = reportingSet {
    this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
    primitive =
      ReportingSetKt.primitive {
        eventGroupKeys +=
          ReportingSetKt.PrimitiveKt.eventGroupKey {
            cmmsDataProviderId = "1235"
            cmmsEventGroupId = "1236"
          }
      }
  }
  return reportingSetsService.createReportingSet(
    createReportingSetRequest {
      this.reportingSet = reportingSet
      this.externalReportingSetId = externalReportingSetId
    }
  )
}

suspend fun createMeasurementConsumer(
  cmmsMeasurementConsumerId: String,
  measurementConsumersService: MeasurementConsumersCoroutineImplBase,
) {
  measurementConsumersService.createMeasurementConsumer(
    measurementConsumer { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
  )
}
