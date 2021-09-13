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
import org.wfanet.measurement.api.v2alpha.AppendLogEntryRequest
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKt.debugLog
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.FinishExchangeStepAttemptRequest
import org.wfanet.measurement.api.v2alpha.GetExchangeStepAttemptRequest
import org.wfanet.measurement.api.v2alpha.ListExchangeStepAttemptsRequest
import org.wfanet.measurement.api.v2alpha.ListExchangeStepAttemptsResponse
import org.wfanet.measurement.api.v2alpha.exchangeStepAttempt
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.grpc.grpcStatusCode
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt as InternalExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetails
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
    return response.toV2Alpha()
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

    // Authorization: ensure that the current gRPC context is authorized to read the ExchangeStep.
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
      when (e.grpcStatusCode()) {
        Status.Code.NOT_FOUND, Status.Code.UNAUTHENTICATED, Status.Code.PERMISSION_DENIED ->
          failGrpc(Status.PERMISSION_DENIED) {
            "FinishExchangeStepAttempt failed: access to ExchangeStep denied or ExchangeStep " +
              "does not exist"
          }
        else -> throw Status.INTERNAL.withCause(e).asRuntimeException()
      }
    }

    val internalRequest = finishExchangeStepAttemptRequest {
      this.externalRecurringExchangeId = externalRecurringExchangeId
      this.date = date
      this.stepIndex = stepIndex
      attemptNumber = exchangeStepAttempt.attemptNumber
      state = request.finalState.toInternal()
      debugLogEntries += request.logEntriesList.toInternal()
    }
    val response = internalExchangeStepAttempts.finishExchangeStepAttempt(internalRequest)
    return response.toV2Alpha()
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

private fun InternalExchangeStepAttempt.toV2Alpha(): ExchangeStepAttempt {
  val key =
    ExchangeStepAttemptKey(
      recurringExchangeId = externalIdToApiId(externalRecurringExchangeId),
      exchangeId = date.toLocalDate().toString(),
      exchangeStepId = stepIndex.toString(),
      exchangeStepAttemptId = this@toV2Alpha.attemptNumber.toString()
    )
  return exchangeStepAttempt {
    name = key.toName()
    attemptNumber = this@toV2Alpha.attemptNumber
    state = this@toV2Alpha.state.toV2Alpha()
    debugLogEntries += details.debugLogEntriesList.map { it.toV2Alpha() }
    startTime = details.startTime
    updateTime = details.updateTime
  }
}

private fun InternalExchangeStepAttempt.State.toV2Alpha(): ExchangeStepAttempt.State {
  return when (this) {
    InternalExchangeStepAttempt.State.STATE_UNSPECIFIED,
    InternalExchangeStepAttempt.State.UNRECOGNIZED ->
      failGrpc(Status.INTERNAL) { "Invalid State: $this" }
    InternalExchangeStepAttempt.State.ACTIVE -> ExchangeStepAttempt.State.ACTIVE
    InternalExchangeStepAttempt.State.SUCCEEDED -> ExchangeStepAttempt.State.SUCCEEDED
    InternalExchangeStepAttempt.State.FAILED -> ExchangeStepAttempt.State.FAILED
    InternalExchangeStepAttempt.State.FAILED_STEP -> ExchangeStepAttempt.State.FAILED_STEP
  }
}

private fun ExchangeStepAttempt.State.toInternal(): InternalExchangeStepAttempt.State {
  return when (this) {
    ExchangeStepAttempt.State.STATE_UNSPECIFIED, ExchangeStepAttempt.State.UNRECOGNIZED ->
      failGrpc { "Invalid State: $this" }
    ExchangeStepAttempt.State.ACTIVE -> InternalExchangeStepAttempt.State.ACTIVE
    ExchangeStepAttempt.State.SUCCEEDED -> InternalExchangeStepAttempt.State.SUCCEEDED
    ExchangeStepAttempt.State.FAILED -> InternalExchangeStepAttempt.State.FAILED
    ExchangeStepAttempt.State.FAILED_STEP -> InternalExchangeStepAttempt.State.FAILED_STEP
  }
}

private fun ExchangeStepAttemptDetails.DebugLog.toV2Alpha(): ExchangeStepAttempt.DebugLog {
  return debugLog {
    time = this@toV2Alpha.time
    message = this@toV2Alpha.message
  }
}

private fun Iterable<ExchangeStepAttempt.DebugLog>.toInternal():
  Iterable<ExchangeStepAttemptDetails.DebugLog> {
  return map { apiProto ->
    ExchangeStepAttemptDetailsKt.debugLog {
      time = apiProto.time
      message = apiProto.message
    }
  }
}
