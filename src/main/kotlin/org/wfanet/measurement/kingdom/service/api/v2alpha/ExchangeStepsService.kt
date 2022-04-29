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

import com.google.protobuf.Timestamp
import io.grpc.Status
import java.time.LocalDate
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepRequest.PartyCase
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeKey
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.GetExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsRequest
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsResponse
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.getProviderFromContext
import org.wfanet.measurement.api.v2alpha.listExchangeStepsResponse
import org.wfanet.measurement.api.v2alpha.validateRequestProvider
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.internal.common.Provider
import org.wfanet.measurement.internal.common.Provider.Type.DATA_PROVIDER
import org.wfanet.measurement.internal.common.Provider.Type.MODEL_PROVIDER
import org.wfanet.measurement.internal.common.provider
import org.wfanet.measurement.internal.kingdom.ExchangeStep as InternalExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamExchangeStepsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepResponse as internalClaimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.streamExchangeStepsRequest

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 100

class ExchangeStepsService(private val internalExchangeSteps: InternalExchangeStepsCoroutineStub) :
  ExchangeStepsCoroutineImplBase() {
  override suspend fun claimReadyExchangeStep(
    request: ClaimReadyExchangeStepRequest
  ): ClaimReadyExchangeStepResponse {
    val provider = validateRequestProvider(getProvider(request))

    val internalRequest = claimReadyExchangeStepRequest { this.provider = provider }
    val internalResponse = internalExchangeSteps.claimReadyExchangeStep(internalRequest)
    if (internalResponse == internalClaimReadyExchangeStepResponse {}) {
      return claimReadyExchangeStepResponse {}
    }
    val externalExchangeStep =
      try {
        internalResponse.exchangeStep.toV2Alpha()
      } catch (e: Throwable) {
        failGrpc(Status.INVALID_ARGUMENT) { e.message ?: "Failed to convert ExchangeStep" }
      }
    val externalExchangeStepAttempt =
      ExchangeStepAttemptKey(
          recurringExchangeId =
            externalIdToApiId(internalResponse.exchangeStep.externalRecurringExchangeId),
          exchangeId = internalResponse.exchangeStep.date.toLocalDate().toString(),
          exchangeStepId = internalResponse.exchangeStep.stepIndex.toString(),
          exchangeStepAttemptId = internalResponse.attemptNumber.toString()
        )
        .toName()
    return claimReadyExchangeStepResponse {
      exchangeStep = externalExchangeStep
      exchangeStepAttempt = externalExchangeStepAttempt
    }
  }

  override suspend fun listExchangeSteps(
    request: ListExchangeStepsRequest
  ): ListExchangeStepsResponse {
    grpcRequire(request.pageSize >= 0) { "Page size cannot be less than 0" }
    grpcRequire(request.filter.recurringExchangeDataProvidersCount == 0) {
      "filter.recurringExchangeDataProviders is not supported yet"
    }
    grpcRequire(request.filter.recurringExchangeModelProvidersCount == 0) {
      "filter.recurringExchangeModelProviders is not supported yet"
    }

    val principal = getProviderFromContext()
    val key =
      grpcRequireNotNull(ExchangeKey.fromName(request.parent)) {
        "Exchange resource name is either unspecified or invalid"
      }
    val pageSize =
      when {
        request.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
        request.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
        else -> request.pageSize
      }

    val streamExchangeStepsRequest = streamExchangeStepsRequest {
      limit = pageSize
      filter = filter {
        this.principal = principal

        if (request.pageToken.isNotBlank()) {
          updatedAfter = Timestamp.parseFrom(request.pageToken.base64UrlDecode())
        }

        if (request.filter.hasDataProvider()) {
          stepProvider = DataProviderKey.fromName(request.filter.dataProvider).toProvider()
        }
        if (request.filter.hasModelProvider()) {
          stepProvider = ModelProviderKey.fromName(request.filter.modelProvider).toProvider()
        }

        externalRecurringExchangeIds += apiIdToExternalId(key.recurringExchangeId)
        if (key.hasExchangeId()) {
          dates += LocalDate.parse(key.exchangeId).toProtoDate()
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
      }
    }

    val results: List<InternalExchangeStep> =
      internalExchangeSteps.streamExchangeSteps(streamExchangeStepsRequest).toList()

    if (results.isEmpty()) {
      return listExchangeStepsResponse {}
    }

    return listExchangeStepsResponse {
      exchangeStep +=
        results.map {
          try {
            it.toV2Alpha()
          } catch (e: Throwable) {
            failGrpc(Status.INVALID_ARGUMENT) {
              e.message ?: "Failed to convert ProtocolConfig ExchangeStep"
            }
          }
        }
      nextPageToken = results.last().updateTime.toByteArray().base64UrlEncode()
    }
  }

  override suspend fun getExchangeStep(request: GetExchangeStepRequest): ExchangeStep {
    TODO("world-federation-of-advertisers/cross-media-measurement#3: implement this")
  }
}

private fun DataProviderKey?.toProvider(): Provider {
  val id = grpcRequireNotNull(this) { "Incorrect data_provider resource name." }.dataProviderId
  return provider {
    type = DATA_PROVIDER
    externalId = apiIdToExternalId(id)
  }
}

private fun ModelProviderKey?.toProvider(): Provider {
  val id = grpcRequireNotNull(this) { "Incorrect model_provider resource name." }.modelProviderId
  return provider {
    type = MODEL_PROVIDER
    externalId = apiIdToExternalId(id)
  }
}

private fun ExchangeKey.hasExchangeId(): Boolean {
  return exchangeId != "-"
}

private fun getProvider(request: ClaimReadyExchangeStepRequest): String {
  return when (request.partyCase) {
    PartyCase.DATA_PROVIDER -> request.dataProvider
    PartyCase.MODEL_PROVIDER -> request.modelProvider
    else ->
      failGrpc(Status.UNAUTHENTICATED) {
        "Caller identity is neither DataProvider nor ModelProvider"
      }
  }
}
