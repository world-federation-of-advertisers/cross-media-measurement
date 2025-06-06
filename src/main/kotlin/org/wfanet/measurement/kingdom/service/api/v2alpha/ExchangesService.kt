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
import io.grpc.StatusException
import java.time.LocalDate
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.AccountPrincipal
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DuchyPrincipal
import org.wfanet.measurement.api.v2alpha.Exchange
import org.wfanet.measurement.api.v2alpha.ExchangeKey
import org.wfanet.measurement.api.v2alpha.ExchangesGrpcKt.ExchangesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.GetExchangeRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.internal.kingdom.Exchange as InternalExchange
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.RecurringExchange as InternalRecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.getExchangeRequest as internalGetExchangeRequest
import org.wfanet.measurement.internal.kingdom.getRecurringExchangeRequest as internalGetRecurringExchangeRequest

class ExchangesService(
  private val internalRecurringExchanges: RecurringExchangesCoroutineStub,
  private val internalExchanges: ExchangesCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ExchangesCoroutineImplBase(coroutineContext) {
  override suspend fun getExchange(request: GetExchangeRequest): Exchange {
    fun permissionDeniedStatus() =
      Status.PERMISSION_DENIED.withDescription(
        "Permission denied on resource ${request.name} (or it might not exist)"
      )

    val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext
    val key =
      grpcRequireNotNull(ExchangeKey.fromName(request.name)) {
        "Resource name not specified or invalid"
      }

    val internalRecurringExchange: InternalRecurringExchange =
      try {
        internalRecurringExchanges.getRecurringExchange(
          internalGetRecurringExchangeRequest {
            externalRecurringExchangeId = ApiId(key.recurringExchangeId).externalId.value
          }
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.PERMISSION_DENIED
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    when (authenticatedPrincipal) {
      is DataProviderPrincipal -> {
        val authenticatedExternalId =
          ApiId(authenticatedPrincipal.resourceKey.dataProviderId).externalId
        if (
          ExternalId(internalRecurringExchange.externalDataProviderId) != authenticatedExternalId
        ) {
          throw permissionDeniedStatus().asRuntimeException()
        }
      }
      is ModelProviderPrincipal -> {
        val authenticatedExternalId =
          ApiId(authenticatedPrincipal.resourceKey.modelProviderId).externalId
        if (
          ExternalId(internalRecurringExchange.externalModelProviderId) != authenticatedExternalId
        ) {
          throw permissionDeniedStatus().asRuntimeException()
        }
      }
      is AccountPrincipal,
      is DuchyPrincipal,
      is MeasurementConsumerPrincipal -> throw permissionDeniedStatus().asRuntimeException()
    }

    val internalExchange: InternalExchange =
      try {
        internalExchanges.getExchange(
          internalGetExchangeRequest {
            externalRecurringExchangeId = apiIdToExternalId(key.recurringExchangeId)
            date = LocalDate.parse(key.exchangeId).toProtoDate()
          }
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    return internalExchange.toExchange()
  }
}
