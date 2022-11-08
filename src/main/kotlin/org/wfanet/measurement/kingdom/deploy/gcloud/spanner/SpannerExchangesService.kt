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

import io.grpc.Status
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.CreateExchangeRequest
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetExchangeRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.PROVIDER_PARAM
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.providerFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateExchange

class SpannerExchangesService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ExchangesCoroutineImplBase() {
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
            AND ${providerFilter(request.provider)}
          """
            .trimIndent()
        )
        bind("external_recurring_exchange_id" to request.externalRecurringExchangeId)
        bind("date" to request.date.toCloudDate())
        bind(PROVIDER_PARAM to request.provider.externalId)
        appendClause("LIMIT 1")
      }
      .execute(client.singleUse())
      .singleOrNull()
      ?.exchange
      ?: failGrpc(Status.NOT_FOUND) { "Exchange not found" }
  }
}
