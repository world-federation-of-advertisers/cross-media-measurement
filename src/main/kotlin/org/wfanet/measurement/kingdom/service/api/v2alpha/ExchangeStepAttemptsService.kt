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

import com.google.type.Date
import io.grpc.Status
import java.time.LocalDate
import org.wfanet.measurement.api.v2alpha.AppendLogEntryRequest
import org.wfanet.measurement.api.v2alpha.CreateExchangeStepAttemptRequest
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.FinishExchangeStepAttemptRequest
import org.wfanet.measurement.api.v2alpha.GetExchangeStepAttemptRequest
import org.wfanet.measurement.api.v2alpha.ListExchangeStepAttemptsRequest
import org.wfanet.measurement.api.v2alpha.ListExchangeStepAttemptsResponse
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.AppendLogEntryRequest as InternalAppendLogEntryRequest
import org.wfanet.measurement.internal.kingdom.CreateExchangeStepAttemptRequest as InternalCreateExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt as InternalExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetails
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub as InternalExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.internal.kingdom.FinishExchangeStepAttemptRequest as InternalFinishExchangeStepAttemptRequest

class ExchangeStepAttemptsService(
  private val internalExchangeStepAttempts: InternalExchangeStepAttemptsCoroutineStub
) : ExchangeStepAttemptsCoroutineImplBase() {
  override suspend fun appendLogEntry(request: AppendLogEntryRequest): ExchangeStepAttempt {
    val internalRequest =
      InternalAppendLogEntryRequest.newBuilder()
        .apply {
          externalRecurringExchangeId = apiIdToExternalId(request.key.recurringExchangeId)
          date = LocalDate.parse(request.key.exchangeId).toProtoDate()
          stepIndex = apiIdToExternalId(request.key.stepId).toInt()
          attemptNumber = apiIdToExternalId(request.key.exchangeStepAttemptId).toInt()
          for (entry in request.logEntriesList) {
            addDebugLogEntriesBuilder().apply {
              time = entry.time
              message = entry.message
            }
          }
        }
        .build()
    val response = internalExchangeStepAttempts.appendLogEntry(internalRequest)
    return response.toV2Alpha()
  }

  override suspend fun createExchangeStepAttempt(
    request: CreateExchangeStepAttemptRequest
  ): ExchangeStepAttempt {
    val sanitizedExchangeStepAttempt =
      request
        .exchangeStepAttempt
        .toBuilder()
        .apply {
          keyBuilder.clearExchangeStepAttemptId()
          clearAttemptNumber()
          state = ExchangeStepAttempt.State.ACTIVE
          clearStartTime()
          clearUpdateTime()
        }
        .build()

    val internalRequest =
      InternalCreateExchangeStepAttemptRequest.newBuilder()
        .setExchangeStepAttempt(sanitizedExchangeStepAttempt.toInternal())
        .build()
    val response = internalExchangeStepAttempts.createExchangeStepAttempt(internalRequest)
    return response.toV2Alpha()
  }

  override suspend fun finishExchangeStepAttempt(
    request: FinishExchangeStepAttemptRequest
  ): ExchangeStepAttempt {
    val internalRequest =
      InternalFinishExchangeStepAttemptRequest.newBuilder()
        .also { builder ->
          with(request.key.toInternalKeyParts()) {
            builder.externalRecurringExchangeId = externalRecurringExchangeId
            builder.date = date
            builder.stepIndex = stepIndex
            builder.attemptNumber = attemptNumber
          }
          builder.state = request.finalState.toInternal()
          builder.addAllDebugLogEntries(request.logEntriesList.toInternal())
        }
        .build()
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

private data class InternalKeyParts(
  val externalRecurringExchangeId: Long,
  val date: Date,
  val stepIndex: Int,
  val attemptNumber: Int
)

private fun ExchangeStepAttempt.Key.toInternalKeyParts(): InternalKeyParts {
  return InternalKeyParts(
    externalRecurringExchangeId = apiIdToExternalId(recurringExchangeId),
    date = LocalDate.parse(exchangeId).toProtoDate(),
    stepIndex = apiIdToExternalId(stepId).toInt(),
    attemptNumber =
      if (exchangeStepAttemptId.isNotBlank()) {
        apiIdToExternalId(exchangeStepAttemptId).toInt()
      } else {
        0
      }
  )
}

private fun InternalExchangeStepAttempt.toV2Alpha(): ExchangeStepAttempt {
  return ExchangeStepAttempt.newBuilder()
    .also { builder ->
      builder.key = toV2AlphaKey()
      builder.attemptNumber = attemptNumber
      builder.state = state.toV2Alpha()
      builder.addAllDebugLogEntries(details.debugLogEntriesList.map { it.toV2Alpha() })
      builder.startTime = details.startTime
      builder.updateTime = details.updateTime
    }
    .build()
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

private fun InternalExchangeStepAttempt.toV2AlphaKey(): ExchangeStepAttempt.Key {
  return ExchangeStepAttempt.Key.newBuilder()
    .apply {
      recurringExchangeId = externalIdToApiId(externalRecurringExchangeId)
      exchangeId = date.toLocalDate().toString()
      stepId = externalIdToApiId(stepIndex.toLong())
      exchangeStepAttemptId = externalIdToApiId(attemptNumber.toLong())
    }
    .build()
}

private fun ExchangeStepAttemptDetails.DebugLog.toV2Alpha(): ExchangeStepAttempt.DebugLog {
  return ExchangeStepAttempt.DebugLog.newBuilder()
    .also {
      it.time = time
      it.message = message
    }
    .build()
}

private fun Iterable<ExchangeStepAttempt.DebugLog>.toInternal():
  Iterable<ExchangeStepAttemptDetails.DebugLog> {
  return map { apiProto ->
    ExchangeStepAttemptDetails.DebugLog.newBuilder()
      .apply {
        time = apiProto.time
        message = apiProto.message
      }
      .build()
  }
}

private fun ExchangeStepAttempt.toInternal(): InternalExchangeStepAttempt {
  return InternalExchangeStepAttempt.newBuilder()
    .also { builder ->
      with(key.toInternalKeyParts()) {
        builder.externalRecurringExchangeId = externalRecurringExchangeId
        builder.date = date
        builder.stepIndex = stepIndex
        builder.attemptNumber = attemptNumber
      }
      builder.state = state.toInternal()
      builder.details = toInternalDetails()
    }
    .build()
}

private fun ExchangeStepAttempt.toInternalDetails(): ExchangeStepAttemptDetails {
  return ExchangeStepAttemptDetails.newBuilder()
    .also { builder ->
      builder.addAllDebugLogEntries(debugLogEntriesList.toInternal())
      builder.startTime = startTime
      builder.updateTime = updateTime
    }
    .build()
}
