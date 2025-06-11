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
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.AccountPrincipal
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DuchyPrincipal
import org.wfanet.measurement.api.v2alpha.ExchangeKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsPageToken
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsPageTokenKt
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsRequest
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.RecurringExchangeParentKey
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.listExchangeStepsPageToken
import org.wfanet.measurement.api.v2alpha.listExchangeStepsResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepResponse as InternalClaimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.ExchangeStep as InternalExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub as InternalRecurringExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamExchangeStepsRequest
import org.wfanet.measurement.internal.kingdom.StreamExchangeStepsRequestKt
import org.wfanet.measurement.internal.kingdom.StreamExchangeStepsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.getRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.streamExchangeStepsRequest

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 100

class ExchangeStepsService(
  private val internalRecurringExchanges: InternalRecurringExchangesCoroutineStub,
  private val internalExchangeSteps: InternalExchangeStepsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ExchangeStepsCoroutineImplBase(coroutineContext) {
  private enum class Permission {
    LIST,
    CLAIM_READY;

    fun deniedStatus(name: String): Status {
      return Status.PERMISSION_DENIED.withDescription(
        "Permission $this denied on resource $name (or it might not exist)"
      )
    }
  }

  override suspend fun claimReadyExchangeStep(
    request: ClaimReadyExchangeStepRequest
  ): ClaimReadyExchangeStepResponse {
    fun permissionDeniedStatus() =
      Permission.CLAIM_READY.deniedStatus("${request.parent}/recurringExchanges")

    val parentKey =
      grpcRequireNotNull(RecurringExchangeParentKey.fromName(request.parent)) {
        "parent not specified or invalid"
      }
    val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext
    if (parentKey != authenticatedPrincipal.resourceKey) {
      throw permissionDeniedStatus().asRuntimeException()
    }

    val internalRequest = claimReadyExchangeStepRequest {
      when (parentKey) {
        is DataProviderKey -> {
          externalDataProviderId = ApiId(parentKey.dataProviderId).externalId.value
        }
        is ModelProviderKey -> {
          externalModelProviderId = ApiId(parentKey.modelProviderId).externalId.value
        }
      }
    }
    val internalResponse: InternalClaimReadyExchangeStepResponse =
      internalExchangeSteps.claimReadyExchangeStep(internalRequest)
    if (!internalResponse.hasExchangeStep()) {
      return ClaimReadyExchangeStepResponse.getDefaultInstance()
    }
    val exchangeStep = internalResponse.exchangeStep.toExchangeStep()
    val exchangeStepAttemptName =
      CanonicalExchangeStepAttemptKey(
          recurringExchangeId =
            externalIdToApiId(internalResponse.exchangeStep.externalRecurringExchangeId),
          exchangeId = internalResponse.exchangeStep.date.toLocalDate().toString(),
          exchangeStepId = internalResponse.exchangeStep.stepIndex.toString(),
          exchangeStepAttemptId = internalResponse.attemptNumber.toString(),
        )
        .toName()
    return claimReadyExchangeStepResponse {
      this.exchangeStep = exchangeStep
      exchangeStepAttempt = exchangeStepAttemptName
    }
  }

  override suspend fun listExchangeSteps(
    request: ListExchangeStepsRequest
  ): ListExchangeStepsResponse {
    grpcRequire(request.pageSize >= 0) { "Page size cannot be less than 0" }

    fun permissionDeniedStatus() = Permission.LIST.deniedStatus("${request.parent}/exchangeSteps")

    val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext
    val parentKey =
      grpcRequireNotNull(ExchangeKey.fromName(request.parent)) {
        "Resource name not specified or invalid"
      }
    val internalRecurringExchange: RecurringExchange =
      try {
        internalRecurringExchanges.getRecurringExchange(
          getRecurringExchangeRequest {
            externalRecurringExchangeId = ApiId(parentKey.recurringExchangeId).externalId.value
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

    val pageToken =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        ListExchangeStepsPageToken.parseFrom(request.pageToken.base64UrlDecode())
      }
    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)

    val streamExchangeStepsRequest = streamExchangeStepsRequest {
      limit = pageSize + 1
      filter = filter {
        externalRecurringExchangeId = internalRecurringExchange.externalRecurringExchangeId

        if (request.filter.dataProvider.isNotEmpty()) {
          val dataProviderKey =
            DataProviderKey.fromName(request.filter.dataProvider)
              ?: throw Status.INVALID_ARGUMENT.withDescription("filter.data_provider invalid")
                .asRuntimeException()
          externalDataProviderId = ApiId(dataProviderKey.dataProviderId).externalId.value
        }

        if (request.filter.modelProvider.isNotEmpty()) {
          val modelProviderKey =
            ModelProviderKey.fromName(request.filter.modelProvider)
              ?: throw Status.INVALID_ARGUMENT.withDescription("filter.model_provider invalid")
                .asRuntimeException()
          externalModelProviderId = ApiId(modelProviderKey.modelProviderId).externalId.value
        }

        if (parentKey.exchangeId != ResourceKey.WILDCARD_ID) {
          dates += LocalDate.parse(parentKey.exchangeId).toProtoDate()
        }

        dates += request.filter.exchangeDatesList
        states +=
          request.filter.statesList.map {
            try {
              it.toInternal()
            } catch (e: Throwable) {
              failGrpc(Status.INVALID_ARGUMENT) {
                e.message ?: "Failed to convert ExchangeStep.State"
              }
            }
          }

        if (pageToken != null) {
          if (
            pageToken.externalRecurringExchangeId != externalRecurringExchangeId ||
              pageToken.externalDataProviderId != externalDataProviderId ||
              pageToken.externalModelProviderId != externalModelProviderId ||
              pageToken.datesList != dates ||
              pageToken.statesList != request.filter.statesList
          ) {
            throw Status.INVALID_ARGUMENT.withDescription(
                "Arguments other than page_size must remain the same for subsequent page requests"
              )
              .asRuntimeException()
          }
          after = pageToken.lastExchangeStep.toOrderedKey()
        }
      }
    }

    val results: List<InternalExchangeStep> =
      internalExchangeSteps.streamExchangeSteps(streamExchangeStepsRequest).toList()

    if (results.isEmpty()) {
      return ListExchangeStepsResponse.getDefaultInstance()
    }

    val limitedResults = results.subList(0, results.size.coerceAtMost(pageSize))
    return listExchangeStepsResponse {
      exchangeSteps += limitedResults.map { it.toExchangeStep() }
      if (results.size > pageSize) {
        nextPageToken =
          buildNextPageToken(
              request.filter,
              streamExchangeStepsRequest.filter,
              limitedResults.last(),
            )
            .toByteString()
            .base64UrlEncode()
      }
    }
  }

  private fun buildNextPageToken(
    filter: ListExchangeStepsRequest.Filter,
    internalFilter: StreamExchangeStepsRequest.Filter,
    lastResult: InternalExchangeStep,
  ) = listExchangeStepsPageToken {
    externalRecurringExchangeId = internalFilter.externalRecurringExchangeId
    dates += internalFilter.datesList
    states += filter.statesList
    externalDataProviderId = internalFilter.externalDataProviderId
    externalModelProviderId = internalFilter.externalModelProviderId

    lastExchangeStep =
      ListExchangeStepsPageTokenKt.previousPageEnd {
        externalRecurringExchangeId = lastResult.externalRecurringExchangeId
        date = lastResult.date
        stepIndex = lastResult.stepIndex
      }
  }
}

private fun ListExchangeStepsPageToken.PreviousPageEnd.toOrderedKey():
  StreamExchangeStepsRequest.OrderedKey {
  val source = this
  return StreamExchangeStepsRequestKt.orderedKey {
    externalRecurringExchangeId = source.externalRecurringExchangeId
    date = source.date
    stepIndex = source.stepIndex
  }
}
