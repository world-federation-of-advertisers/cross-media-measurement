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
import org.wfanet.measurement.api.v2alpha.AppendLogEntryRequest
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.FinishExchangeStepAttemptRequest
import org.wfanet.measurement.api.v2alpha.GetExchangeStepAttemptRequest
import org.wfanet.measurement.api.v2alpha.ListExchangeStepAttemptsRequest
import org.wfanet.measurement.api.v2alpha.ListExchangeStepAttemptsResponse
import org.wfanet.measurement.api.v2alpha.getProviderFromContext
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.grpc.grpcStatusCode
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetailsKt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub as InternalExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.appendLogEntryRequest
import org.wfanet.measurement.internal.kingdom.finishExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.getExchangeStepRequest

class ExchangeStepAttemptsService(
  private val internalExchangeStepAttempts: InternalExchangeStepAttemptsCoroutineStub,
  private val internalExchangeSteps: InternalExchangeStepsCoroutineStub
) : ExchangeStepAttemptsCoroutineImplBase() {

  override suspend fun appendLogEntry(request: AppendLogEntryRequest): ExchangeStepAttempt {
    val exchangeStepAttempt =
      grpcRequireNotNull(ExchangeStepAttemptKey.fromName(request.name)) {
        "Resource name unspecified or invalid."
      }
    val internalRequest = appendLogEntryRequest {
      externalRecurringExchangeId = apiIdToExternalId(exchangeStepAttempt.recurringExchangeId)
      date = LocalDate.parse(exchangeStepAttempt.exchangeId).toProtoDate()
      stepIndex = exchangeStepAttempt.exchangeStepId.toInt()
      attemptNumber = exchangeStepAttempt.exchangeStepAttemptId.toInt()
      for (entry in request.logEntriesList) {
        debugLogEntries +=
          ExchangeStepAttemptDetailsKt.debugLog {
            time = entry.time
            message = entry.message
          }
      }
    }
    val response = internalExchangeStepAttempts.appendLogEntry(internalRequest)
    return try {
      response.toV2Alpha()
    } catch (e: Throwable) {
      failGrpc(Status.INVALID_ARGUMENT) {
        e.message ?: "Failed to convert InternalExchangeStepAttempt"
      }
    }
  }

  override suspend fun finishExchangeStepAttempt(
    request: FinishExchangeStepAttemptRequest
  ): ExchangeStepAttempt {
    val exchangeStepAttempt =
      grpcRequireNotNull(ExchangeStepAttemptKey.fromName(request.name)) {
        "Resource name unspecified or invalid."
      }

    val externalRecurringExchangeId = apiIdToExternalId(exchangeStepAttempt.recurringExchangeId)
    val date = LocalDate.parse(exchangeStepAttempt.exchangeId).toProtoDate()
    val stepIndex = exchangeStepAttempt.exchangeStepId.toInt()
    val provider = getProviderFromContext()

    // Ensure that `provider` is authorized to read the ExchangeStep.
    val exchangeStep =
      try {
        internalExchangeSteps.getExchangeStep(
          getExchangeStepRequest {
            this.externalRecurringExchangeId = externalRecurringExchangeId
            this.date = date
            this.stepIndex = stepIndex
            this.provider = provider
          }
        )
      } catch (e: Exception) {
        if (e.grpcStatusCode() == Status.Code.NOT_FOUND) {
          failGrpc(Status.PERMISSION_DENIED) { "ExchangeStep access denied or does not exist" }
        }
        throw Status.INTERNAL.withCause(e).asRuntimeException()
      }

    // Ensure that `provider` is authorized to modify the ExchangeStep.
    if (exchangeStep.provider != provider) {
      failGrpc(Status.PERMISSION_DENIED) { "ExchangeStep write access denied" }
    }

    val internalRequest = finishExchangeStepAttemptRequest {
      this.provider = provider
      this.externalRecurringExchangeId = externalRecurringExchangeId
      this.date = date
      this.stepIndex = stepIndex
      attemptNumber = exchangeStepAttempt.attemptNumber
      state =
        try {
          request.finalState.toInternal()
        } catch (e: Throwable) {
          failGrpc(Status.INVALID_ARGUMENT) { e.message ?: "Failed to convert FinalState" }
        }
      debugLogEntries += request.logEntriesList.toInternal()
    }
    val response =
      try {
        internalExchangeStepAttempts.finishExchangeStepAttempt(internalRequest)
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.INVALID_ARGUMENT ->
            failGrpc(Status.INVALID_ARGUMENT, ex) { "Date must be provided in the request." }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }
    return try {
      response.toV2Alpha()
    } catch (e: Throwable) {
      failGrpc(Status.INVALID_ARGUMENT) {
        e.message ?: "Failed to convert FinishExchangeStepAttempt"
      }
    }
  }

  override suspend fun getExchangeStepAttempt(
    request: GetExchangeStepAttemptRequest
  ): ExchangeStepAttempt {
    TODO("world-federation-of-advertisers/cross-media-measurement#3: implement this")
  }

  override suspend fun listExchangeStepAttempts(
    request: ListExchangeStepAttemptsRequest
  ): ListExchangeStepAttemptsResponse {
    TODO("world-federation-of-advertisers/cross-media-measurement#3: implement this")
  }
}

private val ExchangeStepAttemptKey.attemptNumber: Int
  get() {
    return if (exchangeStepAttemptId.isNotBlank()) {
      exchangeStepAttemptId.toInt()
    } else {
      0
    }
  }
