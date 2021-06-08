package org.wfanet.measurement.kingdom.service.internal

import java.time.Clock
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.GetMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.GetMeasurementRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase

class SpannerMeasurementsService(
  clock: Clock,
  idGenerator: IdGenerator,
  client: AsyncDatabaseClient
) : MeasurementsCoroutineImplBase() {
  override suspend fun createMeasurement(request: Measurement): Measurement {
    TODO("not implemented yet")
  }
  override suspend fun getMeasurement(request: GetMeasurementRequest): Measurement {
    TODO("not implemented yet")
  }
  override suspend fun getMeasurementByComputationId(
    request: GetMeasurementByComputationIdRequest
  ): Measurement {
    TODO("not implemented yet")
  }
}
