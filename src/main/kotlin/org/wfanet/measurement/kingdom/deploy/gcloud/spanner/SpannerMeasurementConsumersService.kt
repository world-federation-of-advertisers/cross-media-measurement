package org.wfanet.measurement.kingdom.service.internal

import java.time.Clock
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.GetMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase

class SpannerMeasurementConsumersService(
  clock: Clock,
  idGenerator: IdGenerator,
  client: AsyncDatabaseClient
) : MeasurementConsumersCoroutineImplBase() {
  override suspend fun createMeasurementConsumer(
    request: MeasurementConsumer
  ): MeasurementConsumer {
    TODO("not implemented yet")
  }
  override suspend fun getMeasurementConsumer(
    request: GetMeasurementConsumerRequest
  ): MeasurementConsumer {
    TODO("not implemented yet")
  }
}
