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
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.CreateRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.GetRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineImplBase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RecurringExchangeReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateRecurringExchange

class SpannerRecurringExchangesService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : RecurringExchangesCoroutineImplBase() {
  override suspend fun createRecurringExchange(
    request: CreateRecurringExchangeRequest
  ): RecurringExchange {
    return CreateRecurringExchange(request.recurringExchange).execute(client, idGenerator)
  }

  override suspend fun getRecurringExchange(
    request: GetRecurringExchangeRequest
  ): RecurringExchange {
    return RecurringExchangeReader()
      .readByExternalRecurringExchangeId(
        client.singleUse(),
        ExternalId(request.externalRecurringExchangeId)
      )
      ?.recurringExchange
      ?: failGrpc(Status.NOT_FOUND) { "RecurringExchange not found" }
  }
}
