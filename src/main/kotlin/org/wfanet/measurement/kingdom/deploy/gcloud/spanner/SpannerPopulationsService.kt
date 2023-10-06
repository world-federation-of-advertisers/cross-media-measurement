package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.GetPopulationRequest
import org.wfanet.measurement.internal.kingdom.Population
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamPopulationsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamPopulations
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.PopulationReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreatePopulation

class SpannerPopulationsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : PopulationsCoroutineImplBase() {

  override suspend fun createPopulation(request: Population): Population {
    try {
      return CreatePopulation(request).execute(client, idGenerator)
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
    }
  }

  override suspend fun getPopulation(request: GetPopulationRequest): Population {
    return PopulationReader()
      .readByExternalPopulationId(
        client.singleUse(),
        ExternalId(request.externalDataProviderId),
        ExternalId(request.externalPopulationId)
      )
      ?.population ?: failGrpc(Status.NOT_FOUND) { "Population not found" }
  }

  override fun streamPopulations(request: StreamPopulationsRequest): Flow<Population> {
    grpcRequire(request.limit >= 0) { "Limit cannot be less than 0" }
    if (
      request.filter.hasAfter() &&
        (!request.filter.after.hasCreateTime() ||
          request.filter.after.externalDataProviderId == 0L ||
          request.filter.after.externalPopulationId == 0L)
    ) {
      failGrpc(
        Status.INVALID_ARGUMENT,
      ) {
        "Missing After filter fields"
      }
    }
    return StreamPopulations(request.filter, request.limit).execute(client.singleUse()).map {
      it.population
    }
  }
}
