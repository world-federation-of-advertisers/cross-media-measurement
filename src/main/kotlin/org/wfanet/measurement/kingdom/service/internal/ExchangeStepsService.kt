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

package org.wfanet.measurement.kingdom.service.internal

import com.google.type.Date
import io.grpc.Status
import java.time.LocalDate
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.first
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.kingdom.db.getExchangeStepFilter
import org.wfanet.measurement.kingdom.db.streamExchangesFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.GetExchangeStep
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamExchanges
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateExchange
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateExchangeStep
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateExchangeStepAttempt
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.UpdateExchangeStepState
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.UpdateRecurringExchange
import org.wfanet.measurement.kingdom.service.api.v2alpha.toLocalDate
import org.wfanet.measurement.kingdom.service.api.v2alpha.toProtoDate

class ExchangeStepsService(private val client: AsyncDatabaseClient) :
  ExchangeStepsCoroutineImplBase() {

  private fun Date.applyCronSchedule(cronSchedule: String): Date {

    return when (cronSchedule) {
      "DAILY" -> this.toLocalDate().plusDays(1).toProtoDate()
      "WEEKLY" -> this.toLocalDate().plusWeeks(1).toProtoDate()
      "MONTHLY" -> this.toLocalDate().plusMonths(1).toProtoDate()
      "YEARLY" -> this.toLocalDate().plusYears(1).toProtoDate()
      else -> error("Cannot support this.")
    }
  }

  private suspend fun createExchangeSteps(
    recurringExchangeId: Long,
    externalRecurringExchangeId: Long,
    modelProviderId: Long,
    externalModelProviderId: Long?,
    dataProviderId: Long,
    externalDataProviderId: Long?,
    date: Date
  ) {
    // TODO: Create all ExchangeSteps for this Exchange based on the workflow.
    val exchangeStep =
      ExchangeStep.newBuilder()
        .also {
          it.externalRecurringExchangeId = externalRecurringExchangeId
          if (externalModelProviderId != null) {
            it.externalModelProviderId = externalModelProviderId
          }
          if (externalDataProviderId != null) {
            it.externalDataProviderId = externalDataProviderId
          }
          it.date = date
          it.state = ExchangeStep.State.READY
          it.stepIndex = 1
        }
        .build()
    // TODO: Only pick one of model/data provider id based on the workflow.party.
    CreateExchangeStep(
        exchangeStep = exchangeStep,
        recurringExchangeId = recurringExchangeId,
        modelProviderId = modelProviderId,
        dataProviderId = dataProviderId,
      )
      .execute(client)
  }

  private suspend fun createExchangesAndSteps(request: ClaimReadyExchangeStepRequest) {
    // First, stream all exchanges need to be created based on next exchange date.
    val streamFilter =
      streamExchangesFilter(
        externalModelProviderIds =
          if (request.hasExternalModelProviderId())
            listOf(ExternalId(request.externalModelProviderId))
          else emptyList(),
        externalDataProviderIds =
          if (request.hasExternalDataProviderId())
            listOf(ExternalId(request.externalDataProviderId))
          else emptyList(),
        states = listOf(RecurringExchange.State.ACTIVE),
        nextExchangeDate = LocalDate.now().toProtoDate() // NOW
      )
    StreamExchanges(streamFilter, 0).execute(client.singleUse()).collect {
      val recurringExchange = it.recurringExchange
      val recurringExchangeId = it.recurringExchangeId
      val modelProviderId = it.modelProviderId
      val dataProviderId = it.dataProviderId
      val nextExchangeDate = recurringExchange.nextExchangeDate

      // Then, create the Exchange with the date equal to next exchange date.
      CreateExchange(
          recurringExchangeId = recurringExchangeId,
          externalRecurringExchangeId = ExternalId(recurringExchange.externalRecurringExchangeId),
          date = nextExchangeDate
        )
        .execute(client)

      // Calculate the new next exchange date for the recurring exchange.
      val nextNextExchangeDate =
        nextExchangeDate.applyCronSchedule(recurringExchange.details.cronSchedule)

      // Update recurring exchange with new next exchange date. It's state doesn't change.
      UpdateRecurringExchange(
          recurringExchange = recurringExchange,
          recurringExchangeId = recurringExchangeId,
          nextExchangeDate = nextNextExchangeDate,
          state = recurringExchange.state
        )
        .execute(client)

      createExchangeSteps(
        recurringExchangeId = recurringExchangeId,
        externalRecurringExchangeId = recurringExchange.externalRecurringExchangeId,
        modelProviderId = modelProviderId,
        externalModelProviderId = request.externalModelProviderId,
        dataProviderId = dataProviderId,
        externalDataProviderId = request.externalDataProviderId,
        date = nextExchangeDate
      )
    }
  }

  override suspend fun claimReadyExchangeStep(
    request: ClaimReadyExchangeStepRequest
  ): ClaimReadyExchangeStepResponse {
    grpcRequire(request.hasExternalDataProviderId() || request.hasExternalModelProviderId()) {
      "Data Provider id OR Model Provider id must be provided."
    }

    // Create Exchanges and Exchange Steps from the request.
    try {
      createExchangesAndSteps(request)
    } catch (e: NoSuchElementException) {
      throw Status.INVALID_ARGUMENT
        .withCause(e)
        .withDescription("No recurring Exchanges were found.")
        .asRuntimeException()
    }

    // Get the first ExchangeStep with status: READY | READY_FOR_RETRY  by given Provider id.
    val stepFilter =
      getExchangeStepFilter(
        externalModelProviderIds =
          if (request.hasExternalModelProviderId())
            listOf(ExternalId(request.externalModelProviderId))
          else emptyList(),
        externalDataProviderIds =
          if (request.hasExternalDataProviderId())
            listOf(ExternalId(request.externalDataProviderId))
          else emptyList(),
        states = listOf(ExchangeStep.State.READY_FOR_RETRY, ExchangeStep.State.READY)
      )
    val exchangeStepResult =
      try {
        GetExchangeStep(stepFilter).execute(client.singleUse()).first()
      } catch (e: NoSuchElementException) {
        throw Status.INVALID_ARGUMENT
          .withCause(e)
          .withDescription("No Exchange Step was found.")
          .asRuntimeException()
      }

    val exchangeStep = exchangeStepResult.exchangeStep

    // Create an Exchange Step Attempt for this Step.
    val attempt =
      CreateExchangeStepAttempt(
          recurringExchangeId = exchangeStepResult.recurringExchangeId,
          externalRecurringExchangeId = ExternalId(exchangeStep.externalRecurringExchangeId),
          date = exchangeStep.date,
          stepIndex = exchangeStep.stepIndex.toLong()
        )
        .execute(client)

    // Finally, update the Step's status to IN_PROGRESS before return to client.
    val updatedStep =
      UpdateExchangeStepState(
          exchangeStep = exchangeStep,
          recurringExchangeId = exchangeStepResult.recurringExchangeId,
          state = ExchangeStep.State.IN_PROGRESS
        )
        .execute(client)

    // Return ClaimReadyExchangeStepResponse to the client with step and attempt number.
    return ClaimReadyExchangeStepResponse.newBuilder()
      .apply {
        this.exchangeStep = updatedStep
        this.attemptNumber = attempt.attemptNumber
      }
      .build()
  }
}
