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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import io.grpc.Status
import java.time.LocalDate
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.Exchange
import org.wfanet.measurement.api.v2alpha.ExchangeKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangesGrpcKt.ExchangesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.GetExchangeRequest
import org.wfanet.measurement.api.v2alpha.GetExchangeRequest.PartyCase
import org.wfanet.measurement.api.v2alpha.ListExchangesRequest
import org.wfanet.measurement.api.v2alpha.ListExchangesResponse
import org.wfanet.measurement.api.v2alpha.UploadAuditTrailRequest
import org.wfanet.measurement.api.v2alpha.validateRequestProvider
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.internal.kingdom.Exchange as InternalExchange
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.StreamExchangeStepsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.getExchangeRequest
import org.wfanet.measurement.internal.kingdom.streamExchangeStepsRequest
import org.wfanet.measurement.tools.createGraphViz

class ExchangesService(
  private val internalExchanges: ExchangesCoroutineStub,
  private val internalExchangeSteps: ExchangeStepsCoroutineStub
) : ExchangesCoroutineImplBase() {
  override suspend fun getExchange(request: GetExchangeRequest): Exchange {
    val provider = validateRequestProvider(getProvider(request))

    val key = grpcRequireNotNull(ExchangeKey.fromName(request.name))
    val internalExchange =
      internalExchanges.getExchange(
        getExchangeRequest {
          externalRecurringExchangeId = apiIdToExternalId(key.recurringExchangeId)
          date = LocalDate.parse(key.exchangeId).toProtoDate()
          this.provider = provider
        }
      )
    val exchangeSteps =
      internalExchangeSteps.streamExchangeSteps(
        streamExchangeStepsRequest {
          filter { externalRecurringExchangeIds += apiIdToExternalId(key.recurringExchangeId) }
        }
      )

    val externalExchangeWorkflow = getExchangeWorkflow(internalExchange)
    val graphvizString = createGraphViz(externalExchangeWorkflow, exchangeSteps.toList())

    return try {
      internalExchange.toV2Alpha(graphvizString)
    } catch (e: Throwable) {
      failGrpc(Status.INVALID_ARGUMENT) { e.message ?: "Failed to convert InternalExchange" }
    }
  }

  override suspend fun listExchanges(request: ListExchangesRequest): ListExchangesResponse {
    TODO("world-federation-of-advertisers/cross-media-measurement#3: implement this")
  }

  override suspend fun uploadAuditTrail(requests: Flow<UploadAuditTrailRequest>): Exchange {
    TODO("world-federation-of-advertisers/cross-media-measurement#3: implement this")
  }
}

private fun getProvider(request: GetExchangeRequest): String {
  return when (request.partyCase) {
    PartyCase.DATA_PROVIDER -> request.dataProvider
    PartyCase.MODEL_PROVIDER -> request.modelProvider
    else ->
      failGrpc(Status.UNAUTHENTICATED) {
        "Caller identity is neither DataProvider nor ModelProvider"
      }
  }
}

private fun getExchangeWorkflow(internalExchange: InternalExchange): ExchangeWorkflow {
  val recurringExchange =
    RecurringExchange.parser().parseFrom(internalExchange.serializedRecurringExchange)
  val recurringExchangeDetails = recurringExchange.details
  return ExchangeWorkflow.parser().parseFrom(recurringExchangeDetails.externalExchangeWorkflow)
}
