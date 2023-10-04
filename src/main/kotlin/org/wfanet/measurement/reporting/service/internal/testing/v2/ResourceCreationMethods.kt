package org.wfanet.measurement.reporting.service.internal.testing.v2

import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer

suspend fun createMeasurementConsumer(
  cmmsMeasurementConsumerId: String,
  measurementConsumersService: MeasurementConsumersCoroutineImplBase,
) {
  measurementConsumersService.createMeasurementConsumer(
    measurementConsumer { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
  )
}
