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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import com.google.common.base.Optional
import com.google.type.Date
import java.time.LocalDate
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.gcloud.spanner.updateMutation
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeDetails
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetails
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.kingdom.db.getExchangeStepFilter
import org.wfanet.measurement.kingdom.db.streamRecurringExchangesFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.GetExchangeStep
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamRecurringExchanges
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ClaimReadyExchangeStep.Result

class ClaimReadyExchangeStep(
  private val externalModelProviderId: Long?,
  private val externalDataProviderId: Long?
) : SpannerWriter<Result?, Optional<Result>>() {

  private val externalModelProviderIds =
    if (externalModelProviderId == null) emptyList()
    else listOf(ExternalId(externalModelProviderId))
  private val externalDataProviderIds =
    if (externalDataProviderId == null) emptyList() else listOf(ExternalId(externalDataProviderId))

  private data class FirstReadyStep(val exchangeStep: ExchangeStep, val recurringExchangeId: Long?)
  data class Result(val step: ExchangeStep, val attemptNumber: Int)

  override fun ResultScope<Result?>.buildResult(): Optional<Result> {
    val result = checkNotNull(transactionResult) { "No Exchange Steps were found." }
    return Optional.of(Result(result.step, result.attemptNumber))
  }

  override suspend fun TransactionScope.runTransaction(): Result? {
    // Check if any READY | READY_TO_RETRY Exchange Step exists.
    val firstReadyStep = findReadyExchangeStep()
    if (firstReadyStep != null) {
      return firstReadyStep
    }

    // Look for a RecurringExchange that is due for instantiation as an Exchange.
    // If there are any, pick an arbitrary one, create an Exchange and the corresponding
    // ExchangeSteps, and then see if any of those are ready.
    val readyStep = createExchangesAndSteps() ?: return null
    val exchangeStep = readyStep.exchangeStep
    val recurringExchangeId = readyStep.recurringExchangeId ?: return null

    // Create an Exchange Step Attempt for this Step.
    val attempt =
      createExchangeStepAttempt(
        recurringExchangeId = recurringExchangeId,
        externalRecurringExchangeId = ExternalId(exchangeStep.externalRecurringExchangeId),
        date = exchangeStep.date,
        stepIndex = exchangeStep.stepIndex.toLong()
      )

    // Return Result with Exchange Step and Attempt.
    return Result(exchangeStep, attempt.attemptNumber)
  }

  private suspend fun TransactionScope.findReadyExchangeStep(): Result? {
    // Get the first ExchangeStep with status: READY | READY_FOR_RETRY  by given Provider id.
    val stepFilter =
      getExchangeStepFilter(
        externalModelProviderIds = externalModelProviderIds,
        externalDataProviderIds = externalDataProviderIds,
        states = listOf(ExchangeStep.State.READY_FOR_RETRY, ExchangeStep.State.READY)
      )
    val exchangeStepResult =
      GetExchangeStep(stepFilter).execute(transactionContext).singleOrNull() ?: return null

    val exchangeStep = exchangeStepResult.exchangeStep
    val recurringExchangeId = exchangeStepResult.recurringExchangeId

    // Create an Exchange Step Attempt for this Step.
    val attempt =
      createExchangeStepAttempt(
        recurringExchangeId = recurringExchangeId,
        externalRecurringExchangeId = ExternalId(exchangeStep.externalRecurringExchangeId),
        date = exchangeStep.date,
        stepIndex = exchangeStep.stepIndex.toLong()
      )

    // Finally update the Exchange Step status to IN_PROGRESS
    val updatedStep =
      updateExchangeStepState(
        exchangeStep = exchangeStep,
        recurringExchangeId = exchangeStepResult.recurringExchangeId,
        state = ExchangeStep.State.IN_PROGRESS
      )

    return Result(updatedStep, attempt.attemptNumber)
  }

  private suspend fun TransactionScope.createExchangesAndSteps(): FirstReadyStep? {
    // First, retrieve single Recurring Exchange based on the filter below.
    val streamFilter =
      streamRecurringExchangesFilter(
        externalModelProviderIds = externalModelProviderIds,
        externalDataProviderIds = externalDataProviderIds,
        states = listOf(RecurringExchange.State.ACTIVE),
        nextExchangeDateBefore = LocalDate.now().plusDays(1).toProtoDate() // TOMORROW
      )
    val streamResult =
      StreamRecurringExchanges(streamFilter, limit = 1).execute(transactionContext).singleOrNull()
        ?: return null

    val recurringExchange = streamResult.recurringExchange
    val recurringExchangeId = streamResult.recurringExchangeId
    val modelProviderId = streamResult.modelProviderId
    val dataProviderId = streamResult.dataProviderId
    val nextExchangeDate = recurringExchange.nextExchangeDate

    // Then, create the Exchange with the date equal to next exchange date.
    createExchange(
      recurringExchangeId = recurringExchangeId,
      externalRecurringExchangeId = ExternalId(recurringExchange.externalRecurringExchangeId),
      date = nextExchangeDate
    )

    // Calculate the new next exchange date for the recurring exchange.
    val nextNextExchangeDate =
      nextExchangeDate.applyCronSchedule(recurringExchange.details.cronSchedule)

    // Update recurring exchange with new next exchange date. Its state doesn't change.
    updateRecurringExchange(
      recurringExchange = recurringExchange,
      recurringExchangeId = recurringExchangeId,
      nextExchangeDate = nextNextExchangeDate,
      state = recurringExchange.state
    )

    // Create all steps for the Exchange, then return the first step to start.
    val readyStep =
      createExchangeSteps(
        recurringExchangeId = recurringExchangeId,
        externalRecurringExchangeId = recurringExchange.externalRecurringExchangeId,
        modelProviderId = modelProviderId,
        dataProviderId = dataProviderId,
        date = nextExchangeDate
      )

    return FirstReadyStep(readyStep, recurringExchangeId)
  }

  private fun TransactionScope.createExchangeSteps(
    recurringExchangeId: Long,
    externalRecurringExchangeId: Long,
    modelProviderId: Long,
    dataProviderId: Long,
    date: Date
  ): ExchangeStep {
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
          it.state = ExchangeStep.State.IN_PROGRESS
          it.stepIndex = 1
        }
        .build()

    // TODO: Only pick one of model/data provider id based on the workflow.party.
    return createExchangeStep(
      step = exchangeStep,
      recurringExchangeId = recurringExchangeId,
      modelProviderId = modelProviderId,
      dataProviderId = dataProviderId,
    )
  }

  private fun TransactionScope.createExchange(
    recurringExchangeId: Long,
    externalRecurringExchangeId: ExternalId,
    date: Date,
    state: Exchange.State = Exchange.State.ACTIVE
  ): ExternalId {
    // TODO: Set ExchangeDetails with proper Audit trail hash.
    val exchangeDetails = ExchangeDetails.getDefaultInstance()
    insertMutation("Exchanges") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("Date" to date.toCloudDate())
        set("State" to state)
        set("ExchangeDetails" to exchangeDetails)
        setJson("ExchangeDetailsJson" to exchangeDetails)
      }
      .bufferTo(transactionContext)
    return externalRecurringExchangeId
  }

  private fun TransactionScope.updateRecurringExchange(
    recurringExchange: RecurringExchange,
    recurringExchangeId: Long,
    nextExchangeDate: Date,
    state: RecurringExchange.State
  ): RecurringExchange {
    if (recurringExchange.state == state && recurringExchange.nextExchangeDate == nextExchangeDate
    ) {
      return recurringExchange
    }
    require(!recurringExchange.state.isTerminal) {
      "RecurringExchange: $recurringExchange is in a terminal state."
    }
    updateMutation("RecurringExchanges") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("ExternalRecurringExchangeId" to recurringExchange.externalRecurringExchangeId)
        set("State" to state)
        set("NextExchangeDate" to nextExchangeDate.toCloudDate())
        set("RecurringExchangeDetails" to recurringExchange.details)
        setJson("RecurringExchangeDetailsJson" to recurringExchange.details)
      }
      .bufferTo(transactionContext)

    return recurringExchange
      .toBuilder()
      .setNextExchangeDate(nextExchangeDate)
      .setState(state)
      .build()
  }

  private fun TransactionScope.createExchangeStep(
    step: ExchangeStep,
    recurringExchangeId: Long,
    modelProviderId: Long? = null,
    dataProviderId: Long? = null
  ): ExchangeStep {
    val updateTime = Value.COMMIT_TIMESTAMP
    insertMutation("ExchangeSteps") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("Date" to step.date.toCloudDate())
        set("StepIndex" to step.stepIndex.toLong())
        set("State" to step.state)
        set("UpdateTime" to updateTime)
        set("ModelProviderId" to modelProviderId)
        set("DataProviderId" to dataProviderId)
      }
      .bufferTo(transactionContext)

    return step.toBuilder().setUpdateTime(updateTime.toProto()).build()
  }

  private suspend fun TransactionScope.createExchangeStepAttempt(
    recurringExchangeId: Long,
    externalRecurringExchangeId: ExternalId,
    date: Date,
    stepIndex: Long
  ): ExchangeStepAttempt {
    // TODO: Set ExchangeStepAttemptDetails with suitable fields.
    val details = ExchangeStepAttemptDetails.getDefaultInstance()
    val exchangeStepAttempt =
      ExchangeStepAttempt.newBuilder()
        .also {
          it.date = date
          it.details = details
          it.state = ExchangeStepAttempt.State.ACTIVE
          it.stepIndex = stepIndex.toInt()
          it.attemptNumber = findAttemptIndex(recurringExchangeId, date, stepIndex).toInt()
          it.externalRecurringExchangeId = externalRecurringExchangeId.value
        }
        .build()
    insertMutation("ExchangeStepAttempts") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("Date" to date.toCloudDate())
        set("StepIndex" to stepIndex)
        set("AttemptIndex" to exchangeStepAttempt.attemptNumber.toLong())
        set("ExchangeStepAttemptDetails" to details)
        setJson("ExchangeStepAttemptDetailsJson" to details)
      }
      .bufferTo(transactionContext)

    return exchangeStepAttempt
  }

  private suspend fun TransactionScope.findAttemptIndex(
    recurringExchangeId: Long,
    date: Date,
    stepIndex: Long
  ): Long {
    val sql =
      """
      SELECT IFNULL(MAX(AttemptIndex), 0) AS MaxAttemptIndex
      FROM ExchangeStepAttempts
      WHERE ExchangeStepAttempts.RecurringExchangeId = @recurring_exchange_id
      AND ExchangeStepAttempts.Date = @date
      AND ExchangeStepAttempts.StepIndex = @step_index
      """.trimIndent()

    val statement: Statement =
      Statement.newBuilder(sql)
        .bind("recurring_exchange_id")
        .to(recurringExchangeId)
        .bind("date")
        .to(date.toCloudDate())
        .bind("step_index")
        .to(stepIndex)
        .build()
    val row: Struct = transactionContext.executeQuery(statement).single()

    return row.getLong("MaxAttemptIndex") + 1L
  }

  private fun TransactionScope.updateExchangeStepState(
    exchangeStep: ExchangeStep,
    recurringExchangeId: Long,
    state: ExchangeStep.State
  ): ExchangeStep {
    if (exchangeStep.state == state) {
      return exchangeStep
    }
    require(!exchangeStep.state.isTerminal) {
      "ExchangeStep: $exchangeStep is in a terminal state."
    }
    val updateTime = Value.COMMIT_TIMESTAMP
    updateMutation("ExchangeSteps") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("Date" to exchangeStep.date.toCloudDate())
        set("StepIndex" to exchangeStep.stepIndex.toLong())
        set("State" to state)
        set("UpdateTime" to updateTime)
      }
      .bufferTo(transactionContext)

    return exchangeStep.toBuilder().setState(state).setUpdateTime(updateTime.toProto()).build()
  }
}

internal fun Date.applyCronSchedule(cronSchedule: String): Date {
  return when (cronSchedule) {
    "DAILY" -> this.toLocalDate().plusDays(1).toProtoDate()
    "WEEKLY" -> this.toLocalDate().plusWeeks(1).toProtoDate()
    "MONTHLY" -> this.toLocalDate().plusMonths(1).toProtoDate()
    "YEARLY" -> this.toLocalDate().plusYears(1).toProtoDate()
    else -> error("Cannot support this.")
  }
}

private val RecurringExchange.State.isTerminal: Boolean
  get() =
    when (this) {
      RecurringExchange.State.ACTIVE -> false
      RecurringExchange.State.STATE_UNSPECIFIED,
      RecurringExchange.State.UNRECOGNIZED,
      RecurringExchange.State.RETIRED -> true
    }

private val ExchangeStep.State.isTerminal: Boolean
  get() =
    when (this) {
      ExchangeStep.State.READY,
      ExchangeStep.State.READY_FOR_RETRY,
      ExchangeStep.State.IN_PROGRESS -> false
      ExchangeStep.State.BLOCKED,
      ExchangeStep.State.SUCCEEDED,
      ExchangeStep.State.FAILED,
      ExchangeStep.State.UNRECOGNIZED,
      ExchangeStep.State.STATE_UNSPECIFIED -> true
    }
