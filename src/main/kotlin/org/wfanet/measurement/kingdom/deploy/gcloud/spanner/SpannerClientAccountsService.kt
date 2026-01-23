/*
 * Copyright 2026 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.internal.kingdom.ClientAccount
import org.wfanet.measurement.internal.kingdom.ClientAccountsGrpcKt.ClientAccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.CreateClientAccountRequest
import org.wfanet.measurement.internal.kingdom.DeleteClientAccountRequest
import org.wfanet.measurement.internal.kingdom.GetClientAccountRequest
import org.wfanet.measurement.internal.kingdom.StreamClientAccountsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ClientAccountNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamClientAccounts
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ClientAccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateClientAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.DeleteClientAccount

class SpannerClientAccountsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ClientAccountsCoroutineImplBase(coroutineContext) {

  override suspend fun createClientAccount(request: CreateClientAccountRequest): ClientAccount {
    try {
      return CreateClientAccount(request.clientAccount).execute(client, idGenerator)
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
    }
  }

  override suspend fun getClientAccount(request: GetClientAccountRequest): ClientAccount {
    return ClientAccountReader()
      .readByExternalId(
        client.singleUse(),
        ExternalId(request.externalMeasurementConsumerId),
        ExternalId(request.externalClientAccountId),
      )
      ?.clientAccount
      ?: throw ClientAccountNotFoundException(
          ExternalId(request.externalMeasurementConsumerId),
          ExternalId(request.externalClientAccountId),
        )
        .asStatusRuntimeException(Status.Code.NOT_FOUND, "ClientAccount not found.")
  }

  override suspend fun deleteClientAccount(request: DeleteClientAccountRequest): ClientAccount {
    try {
      return DeleteClientAccount(
          ExternalId(request.externalMeasurementConsumerId),
          ExternalId(request.externalClientAccountId),
        )
        .execute(client, idGenerator)
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
    } catch (e: ClientAccountNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "ClientAccount not found.")
    }
  }

  override fun streamClientAccounts(request: StreamClientAccountsRequest): Flow<ClientAccount> {
    grpcRequire(request.limit >= 0) { "Limit cannot be less than 0" }
    if (
      request.filter.hasAfter() &&
        (!request.filter.after.hasCreateTime() ||
          request.filter.after.externalMeasurementConsumerId == 0L ||
          request.filter.after.externalClientAccountId == 0L)
    ) {
      failGrpc(Status.INVALID_ARGUMENT) { "Missing After filter fields" }
    }
    return StreamClientAccounts(request.filter, request.limit).execute(client.singleUse()).map {
      it.clientAccount
    }
  }
}



