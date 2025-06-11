// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import com.google.protobuf.Empty
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.BatchDeleteExchangesRequest
import org.wfanet.measurement.internal.kingdom.CreateExchangeRequest
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetExchangeRequest
import org.wfanet.measurement.internal.kingdom.StreamExchangesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ExchangeNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamExchanges
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.BatchDeleteExchanges
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateExchange

private const val MAX_BATCH_DELETE = 1000

class SpannerExchangesService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ExchangesCoroutineImplBase(coroutineContext) {
  override suspend fun createExchange(request: CreateExchangeRequest): Exchange {
    return CreateExchange(request.exchange).execute(client, idGenerator)
  }

  override suspend fun getExchange(request: GetExchangeRequest): Exchange {
    return ExchangeReader()
      .fillStatementBuilder {
        appendClause(
          """
          WHERE RecurringExchanges.ExternalRecurringExchangeId = @external_recurring_exchange_id
            AND Exchanges.Date = @date
          """
            .trimIndent()
        )
        bind("external_recurring_exchange_id" to request.externalRecurringExchangeId)
        bind("date" to request.date.toCloudDate())
        appendClause("LIMIT 1")
      }
      .execute(client.singleUse())
      .singleOrNull()
      ?.exchange
      ?: throw ExchangeNotFoundException(
          ExternalId(request.externalRecurringExchangeId),
          request.date,
        )
        .asStatusRuntimeException(Status.Code.NOT_FOUND, "Exchange not found")
  }

  override fun streamExchanges(request: StreamExchangesRequest): Flow<Exchange> {
    return StreamExchanges(request.filter, request.limit).execute(client.singleUse()).map {
      it.exchange
    }
  }

  override suspend fun batchDeleteExchanges(request: BatchDeleteExchangesRequest): Empty {
    grpcRequire(request.requestsList.size <= MAX_BATCH_DELETE) {
      "number of requested Exchanges exceeds limit: $MAX_BATCH_DELETE"
    }
    for (exchangeDeletionRequest in request.requestsList) {
      grpcRequire(exchangeDeletionRequest.externalRecurringExchangeId != 0L) {
        "external_recurring_exchange_id not specified"
      }
    }
    try {
      return BatchDeleteExchanges(request).execute(client, idGenerator)
    } catch (e: ExchangeNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Exchange not found")
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error")
    }
  }
}
