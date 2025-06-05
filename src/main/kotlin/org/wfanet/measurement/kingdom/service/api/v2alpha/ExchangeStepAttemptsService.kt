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
import io.grpc.StatusException
import java.io.IOException
import java.lang.NumberFormatException
import java.time.LocalDate
import java.time.format.DateTimeParseException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.AccountPrincipal
import org.wfanet.measurement.api.v2alpha.AppendExchangeStepAttemptLogEntryRequest
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DuchyPrincipal
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.FinishExchangeStepAttemptRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt as InternalExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetailsKt as InternalExchangeStepAttemptDetails
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub as InternalExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.appendLogEntryRequest as internalAppendLogEntryRequest
import org.wfanet.measurement.internal.kingdom.finishExchangeStepAttemptRequest as internalFinishExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.getExchangeStepRequest as internalGetExchangeStepRequest

class ExchangeStepAttemptsService(
  private val internalExchangeStepAttempts: InternalExchangeStepAttemptsCoroutineStub,
  private val internalExchangeSteps: InternalExchangeStepsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ExchangeStepAttemptsCoroutineImplBase(coroutineContext) {

  private enum class Permission {
    FINISH,
    APPEND_LOG_ENTRY;

    fun deniedStatus(name: String): Status {
      return Status.PERMISSION_DENIED.withDescription(
        "Permission $this denied on resource $name (or it might not exist)"
      )
    }
  }

  private data class ExternalIds(
    val externalRecurringExchangeId: ExternalId,
    val date: Date,
    val stepIndex: Int,
    val attemptNumber: Int,
  )

  override suspend fun appendExchangeStepAttemptLogEntry(
    request: AppendExchangeStepAttemptLogEntryRequest
  ): ExchangeStepAttempt {
    fun permissionDeniedStatus() = Permission.APPEND_LOG_ENTRY.deniedStatus(request.name)

    val key =
      grpcRequireNotNull(ExchangeStepAttemptKey.fromName(request.name)) {
        "Resource name unspecified or invalid."
      }
    val externalIds: ExternalIds = parseExternalIds(key)

    checkAuth(externalIds, ::permissionDeniedStatus)

    val internalRequest = internalAppendLogEntryRequest {
      externalRecurringExchangeId = externalIds.externalRecurringExchangeId.value
      date = externalIds.date
      stepIndex = externalIds.stepIndex
      attemptNumber = externalIds.attemptNumber
      for (entry in request.logEntriesList) {
        debugLogEntries +=
          InternalExchangeStepAttemptDetails.debugLog {
            time = entry.entryTime
            message = entry.message
          }
      }
    }
    val internalResponse: InternalExchangeStepAttempt =
      try {
        internalExchangeStepAttempts.appendLogEntry(internalRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }
    return internalResponse.toExchangeStepAttempt()
  }

  override suspend fun finishExchangeStepAttempt(
    request: FinishExchangeStepAttemptRequest
  ): ExchangeStepAttempt {
    fun permissionDeniedStatus() = Permission.FINISH.deniedStatus(request.name)

    val key =
      grpcRequireNotNull(ExchangeStepAttemptKey.fromName(request.name)) {
        "Resource name unspecified or invalid."
      }
    val externalIds: ExternalIds = parseExternalIds(key)

    checkAuth(externalIds, ::permissionDeniedStatus)

    val internalRequest = internalFinishExchangeStepAttemptRequest {
      externalRecurringExchangeId = externalIds.externalRecurringExchangeId.value
      date = externalIds.date
      stepIndex = externalIds.stepIndex
      attemptNumber = externalIds.attemptNumber
      state = request.finalState.toInternal()
      debugLogEntries += request.logEntriesList.toInternal()
    }
    val internalResponse =
      try {
        internalExchangeStepAttempts.finishExchangeStepAttempt(internalRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }
    return internalResponse.toExchangeStepAttempt()
  }

  /**
   * Parses [ExternalIds] from [key].
   *
   * @throws io.grpc.StatusRuntimeException with [Status.Code.INVALID_ARGUMENT] when [key] cannot be
   *   parsed
   */
  private fun parseExternalIds(key: ExchangeStepAttemptKey): ExternalIds {
    fun parseError(cause: Throwable) =
      Status.INVALID_ARGUMENT.withCause(cause)
        .withDescription("Resource name is malformed")
        .asRuntimeException()

    return try {
      ExternalIds(
        ApiId(key.recurringExchangeId).externalId,
        LocalDate.parse(key.exchangeId).toProtoDate(),
        key.exchangeStepId.toInt(),
        key.exchangeStepAttemptId.toInt(),
      )
    } catch (e: DateTimeParseException) {
      throw parseError(e)
    } catch (e: NumberFormatException) {
      throw parseError(e)
    } catch (e: IOException) {
      throw parseError(e)
    }
  }

  /**
   * Checks that the authenticated principal is authorized to perform an operation on the
   * [ExchangeStepAttempt] identified by [externalIds].
   *
   * @throws io.grpc.StatusRuntimeException
   */
  private suspend fun checkAuth(externalIds: ExternalIds, permissionDeniedStatus: () -> Status) {
    val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext
    val internalExchangeStep =
      try {
        internalExchangeSteps.getExchangeStep(
          internalGetExchangeStepRequest {
            externalRecurringExchangeId = externalIds.externalRecurringExchangeId.value
            date = externalIds.date
            stepIndex = externalIds.stepIndex
          }
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.PERMISSION_DENIED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }
    when (authenticatedPrincipal) {
      is DataProviderPrincipal -> {
        val externalId = ApiId(authenticatedPrincipal.resourceKey.dataProviderId).externalId
        if (internalExchangeStep.externalDataProviderId != externalId.value) {
          throw permissionDeniedStatus().asRuntimeException()
        }
      }
      is ModelProviderPrincipal -> {
        val externalId = ApiId(authenticatedPrincipal.resourceKey.modelProviderId).externalId
        if (internalExchangeStep.externalModelProviderId != externalId.value) {
          throw permissionDeniedStatus().asRuntimeException()
        }
      }
      is AccountPrincipal,
      is DuchyPrincipal,
      is MeasurementConsumerPrincipal -> throw permissionDeniedStatus().asRuntimeException()
    }
  }
}
