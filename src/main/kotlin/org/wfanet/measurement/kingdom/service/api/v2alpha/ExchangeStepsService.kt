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
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.GetExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.exchangeStep
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.internal.kingdom.ExchangeStep as InternalExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepResponse as internalClaimReadyExchangeStepResponse

class ExchangeStepsService(private val internalExchangeSteps: InternalExchangeStepsCoroutineStub) :
  ExchangeStepsCoroutineImplBase() {
  override suspend fun claimReadyExchangeStep(
    request: ClaimReadyExchangeStepRequest
  ): ClaimReadyExchangeStepResponse {
    // TODO(@efoxepstein): ensure the request's provider matches the Principal
    val internalRequest = claimReadyExchangeStepRequest { provider = getProviderFromContext() }

    val internalResponse = internalExchangeSteps.claimReadyExchangeStep(internalRequest)
    if (internalResponse == internalClaimReadyExchangeStepResponse {}) {
      return claimReadyExchangeStepResponse {}
    }
    val externalExchangeStep = internalResponse.exchangeStep.toV2Alpha()
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

  override suspend fun getExchangeStep(request: GetExchangeStepRequest): ExchangeStep {
    TODO("world-federation-of-advertisers/cross-media-measurement#3: implement this")
  }
}

private fun InternalExchangeStep.toV2Alpha(): ExchangeStep {
  val exchangeStepKey =
    ExchangeStepKey(
      recurringExchangeId = externalIdToApiId(externalRecurringExchangeId),
      exchangeId = date.toLocalDate().toString(),
      exchangeStepId = stepIndex.toString()
    )
  return exchangeStep {
    name = exchangeStepKey.toName()
    state = v2AlphaState
    stepIndex = this@toV2Alpha.stepIndex
    exchangeDate = date
    serializedExchangeWorkflow = this@toV2Alpha.serializedExchangeWorkflow
  }
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
