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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.CreatePopulationRequest
import org.wfanet.measurement.internal.kingdom.GetPopulationRequest
import org.wfanet.measurement.internal.kingdom.Population
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamPopulationsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.PopulationNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamPopulations
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.PopulationReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreatePopulation

class SpannerPopulationsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : PopulationsCoroutineImplBase(coroutineContext) {

  override suspend fun createPopulation(request: CreatePopulationRequest): Population {
    try {
      return CreatePopulation(request.population, request.requestId.ifEmpty { null })
        .execute(client, idGenerator)
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
    }
  }

  override suspend fun getPopulation(request: GetPopulationRequest): Population {
    return PopulationReader()
      .readByExternalPopulationId(
        client.singleUse(),
        ExternalId(request.externalDataProviderId),
        ExternalId(request.externalPopulationId),
      )
      ?.population
      ?: throw PopulationNotFoundException(
          ExternalId(request.externalDataProviderId),
          ExternalId(request.externalPopulationId),
        )
        .asStatusRuntimeException(Status.Code.NOT_FOUND, "Population not found.")
  }

  override fun streamPopulations(request: StreamPopulationsRequest): Flow<Population> {
    grpcRequire(request.limit >= 0) { "Limit cannot be less than 0" }
    if (
      request.filter.hasAfter() &&
        (!request.filter.after.hasCreateTime() ||
          request.filter.after.externalDataProviderId == 0L ||
          request.filter.after.externalPopulationId == 0L)
    ) {
      failGrpc(Status.INVALID_ARGUMENT) { "Missing After filter fields" }
    }
    return StreamPopulations(request.filter, request.limit).execute(client.singleUse()).map {
      it.population
    }
  }
}
