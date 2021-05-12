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
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepRequest.PartyCase
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.GetExchangeStepRequest
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepRequest as InternalClaimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.ExchangeStep as InternalExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub

class ExchangeStepsService(private val internalExchangeSteps: InternalExchangeStepsCoroutineStub) :
  ExchangeStepsCoroutineImplBase() {
  override suspend fun claimReadyExchangeStep(
    request: ClaimReadyExchangeStepRequest
  ): ClaimReadyExchangeStepResponse {
    val internalRequest =
      InternalClaimReadyExchangeStepRequest.newBuilder()
        .apply {
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
          when (request.partyCase) {
            PartyCase.DATA_PROVIDER ->
              externalDataProviderId = apiIdToExternalId(request.dataProvider.dataProviderId)
            PartyCase.MODEL_PROVIDER ->
              externalModelProviderId = apiIdToExternalId(request.modelProvider.modelProviderId)
            PartyCase.PARTY_NOT_SET -> failGrpc { "Party not set" }
          }
        }
        .build()

    val internalResponse = internalExchangeSteps.claimReadyExchangeStep(internalRequest)
    return ClaimReadyExchangeStepResponse.newBuilder()
      .apply {
        exchangeStep = internalResponse.exchangeStep.toV2Alpha()
        exchangeStepAttemptBuilder.apply {
          recurringExchangeId = exchangeStep.key.recurringExchangeId
          exchangeId = exchangeStep.key.exchangeId
          stepId = exchangeStep.key.exchangeStepId
          exchangeStepAttemptId = externalIdToApiId(internalResponse.attemptNumber.toLong())
        }
      }
      .build()
  }

  override suspend fun getExchangeStep(request: GetExchangeStepRequest): ExchangeStep {
    TODO("world-federation-of-advertisers/cross-media-measurement#3: implement this")
  }
}

private fun InternalExchangeStep.toV2Alpha(): ExchangeStep {
  return ExchangeStep.newBuilder()
    .also {
      it.key = toV2AlphaKey()
      it.state = v2AlphaState
      // TODO(world-federation-of-advertisers/cross-media-measurement#3): add remaining fields
    }
    .build()
}

private val InternalExchangeStep.v2AlphaState: ExchangeStep.State
  get() {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (this.state) {
      InternalExchangeStep.State.BLOCKED -> ExchangeStep.State.BLOCKED
      InternalExchangeStep.State.READY -> ExchangeStep.State.READY
      InternalExchangeStep.State.READY_FOR_RETRY -> ExchangeStep.State.READY_FOR_RETRY
      InternalExchangeStep.State.IN_PROGRESS -> ExchangeStep.State.IN_PROGRESS
      InternalExchangeStep.State.SUCCEEDED -> ExchangeStep.State.SUCCEEDED
      InternalExchangeStep.State.FAILED -> ExchangeStep.State.FAILED
      InternalExchangeStep.State.STATE_UNSPECIFIED, InternalExchangeStep.State.UNRECOGNIZED ->
        failGrpc(Status.INTERNAL) { "Invalid state: $this" }
    }
  }

fun InternalExchangeStep.toV2AlphaKey(): ExchangeStep.Key {
  return ExchangeStep.Key.newBuilder()
    .also {
      it.recurringExchangeId = externalIdToApiId(externalRecurringExchangeId)
      it.exchangeId = date.toLocalDate().toString()
      it.exchangeStepId = externalIdToApiId(stepIndex.toLong())
    }
    .build()
}
