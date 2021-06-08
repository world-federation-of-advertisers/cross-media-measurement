package org.wfanet.measurement.kingdom.service.internal

import java.time.Clock
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetDataProviderRequest

class SpannerDataProvidersService(
  clock: Clock,
  idGenerator: IdGenerator,
  client: AsyncDatabaseClient
) : DataProvidersCoroutineImplBase() {
  override suspend fun createDataProvider(request: DataProvider): DataProvider {
    TODO("not implemented yet")
  }
  override suspend fun getDataProvider(request: GetDataProviderRequest): DataProvider {
    TODO("not implemented yet")
  }
}
