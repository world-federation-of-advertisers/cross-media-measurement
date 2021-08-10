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
import com.google.cloud.spanner.Value
import com.google.type.Date
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.gcloud.spanner.updateMutation
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetails
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.FinishExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.FinishExchangeStepAttemptRequest.PartyCase
import org.wfanet.measurement.kingdom.db.getExchangeStepFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.GetExchangeStep
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepAttemptReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RecurringExchangeReader

class FinishExchangeStepAttempt(private val request: FinishExchangeStepAttemptRequest) :
  SimpleSpannerWriter<ExchangeStepAttempt>() {
  private val externalModelProviderIds =
    if (request.partyCase == PartyCase.EXTERNAL_MODEL_PROVIDER_ID)
      listOf(ExternalId(request.externalModelProviderId))
    else emptyList()
  private val externalDataProviderIds =
    if (request.partyCase == PartyCase.EXTERNAL_DATA_PROVIDER_ID)
      listOf(ExternalId(request.externalDataProviderId))
    else emptyList()
  private val externalRecurringExchangeId = request.externalRecurringExchangeId
  private val reqDate = request.date
  private val reqStepIndex = request.stepIndex
  private val reqAttemptIndex = request.attemptNumber

  override suspend fun TransactionScope.runTransaction(): ExchangeStepAttempt {
    val stepAttemptResult =
      requireNotNull(
        getExchangeStepAttempt(
          externalRecurringExchangeId = externalRecurringExchangeId,
          date = reqDate,
          stepIndex = reqStepIndex,
          attemptIndex = reqAttemptIndex,
        )
      ) { "Attempt for Step: $reqStepIndex not found." }
    val stepResult =
      requireNotNull(
        GetExchangeStep(
            getExchangeStepFilter(
              externalDataProviderIds = externalDataProviderIds,
              externalModelProviderIds = externalModelProviderIds,
              recurringExchangeId = stepAttemptResult.recurringExchangeId,
              date = reqDate,
              stepIndex = reqStepIndex.toLong(),
            )
          )
          .execute(transactionContext)
          .singleOrNull()
      ) { "Step: $reqStepIndex not found." }

    return when (request.state) {
      ExchangeStepAttempt.State.SUCCEEDED -> succeed(stepResult, stepAttemptResult)
      ExchangeStepAttempt.State.FAILED -> temporarilyFail(stepResult, stepAttemptResult)
      ExchangeStepAttempt.State.FAILED_STEP -> permanentlyFail(stepResult, stepAttemptResult)
      else -> throw IllegalArgumentException("Exchange Step Attempt State is not set.")
    }
  }

  private suspend fun TransactionScope.succeed(
    stepResult: ExchangeStepReader.Result,
    stepAttemptResult: ExchangeStepAttemptReader.Result
  ): ExchangeStepAttempt {
    val exchangeStepAttempt = stepAttemptResult.exchangeStepAttempt
    val recurringExchangeId = stepAttemptResult.recurringExchangeId
    val exchangeStep = stepResult.exchangeStep

    updateExchangeStepState(
      exchangeStep = exchangeStep,
      recurringExchangeId = recurringExchangeId,
      state = ExchangeStep.State.SUCCEEDED
    )

    val workflow =
      RecurringExchangeReader()
        .readExternalId(transactionContext, ExternalId(externalRecurringExchangeId))
        .recurringExchange
        .details
        .exchangeWorkflow

    val steps =
      findReadyExchangeSteps(
        workflow = workflow,
        recurringExchangeId = recurringExchangeId,
        date = reqDate
      )
    updateExchangeStepsToReady(
      steps = steps,
      recurringExchangeId = recurringExchangeId,
      date = reqDate
    )

    return updateExchangeStepAttempt(
      recurringExchangeId = recurringExchangeId,
      exchangeStepAttempt = exchangeStepAttempt,
      state = ExchangeStepAttempt.State.SUCCEEDED
    )
  }

  private fun TransactionScope.temporarilyFail(
    stepResult: ExchangeStepReader.Result,
    stepAttemptResult: ExchangeStepAttemptReader.Result
  ): ExchangeStepAttempt {
    val exchangeStepAttempt = stepAttemptResult.exchangeStepAttempt
    val recurringExchangeId = stepAttemptResult.recurringExchangeId
    val exchangeStep = stepResult.exchangeStep

    updateExchangeStepState(
      exchangeStep = exchangeStep,
      recurringExchangeId = recurringExchangeId,
      state = ExchangeStep.State.READY_FOR_RETRY
    )

    return updateExchangeStepAttempt(
      recurringExchangeId = recurringExchangeId,
      exchangeStepAttempt = exchangeStepAttempt,
      state = ExchangeStepAttempt.State.FAILED
    )
  }

  private fun TransactionScope.permanentlyFail(
    stepResult: ExchangeStepReader.Result,
    stepAttemptResult: ExchangeStepAttemptReader.Result
  ): ExchangeStepAttempt {
    val exchangeStepAttempt = stepAttemptResult.exchangeStepAttempt
    val recurringExchangeId = stepAttemptResult.recurringExchangeId
    val exchangeStep = stepResult.exchangeStep

    updateExchangeStepState(
      exchangeStep = exchangeStep,
      recurringExchangeId = recurringExchangeId,
      state = ExchangeStep.State.FAILED
    )

    updateExchangeState(
      recurringExchangeId = recurringExchangeId,
      date = exchangeStep.date,
      state = Exchange.State.FAILED
    )

    return updateExchangeStepAttempt(
      recurringExchangeId = recurringExchangeId,
      exchangeStepAttempt = exchangeStepAttempt,
      state = ExchangeStepAttempt.State.FAILED_STEP
    )
  }

  private suspend fun TransactionScope.getCompletedExchangeSteps(
    recurringExchangeId: Long,
    date: Date
  ): Set<Int> {
    val sql =
      """
      SELECT
       ExchangeSteps.StepIndex
      FROM ExchangeSteps
      WHERE ExchangeSteps.RecurringExchangeId = @recurring_exchange_id
      AND ExchangeSteps.Date = @date
      AND ExchangeSteps.State = @state
      ORDER BY ExchangeSteps.StepIndex
      """.trimIndent()
    val statement: Statement =
      Statement.newBuilder(sql)
        .bind("recurring_exchange_id")
        .to(recurringExchangeId)
        .bind("date")
        .to(date.toCloudDate())
        .bind("state")
        .toProtoEnum(ExchangeStep.State.SUCCEEDED)
        .build()
    val result = mutableSetOf<Int>()
    transactionContext.executeQuery(statement).collect {
      result.add(it.getLong("StepIndex").toInt())
    }
    return result
  }

  private suspend fun TransactionScope.findReadyExchangeSteps(
    workflow: ExchangeWorkflow,
    recurringExchangeId: Long,
    date: Date
  ): List<ExchangeWorkflow.Step> {
    val completedStepIndexes = getCompletedExchangeSteps(recurringExchangeId, date)
    return workflow.stepsList.filter { step ->
      step.prerequisiteStepIndicesCount > 0 &&
        step.prerequisiteStepIndicesList.all { it in completedStepIndexes }
    }
  }

  private suspend fun TransactionScope.getExchangeStepAttempt(
    externalRecurringExchangeId: Long,
    date: Date,
    stepIndex: Int,
    attemptIndex: Int
  ): ExchangeStepAttemptReader.Result? {
    return ExchangeStepAttemptReader()
      .withBuilder {
        appendClause(
          "WHERE RecurringExchanges.ExternalRecurringExchangeId = @external_recurring_exchange_id"
        )
        appendClause("AND ExchangeStepAttempts.Date = @date")
        appendClause("AND ExchangeStepAttempts.StepIndex = @step_index")
        appendClause("AND ExchangeStepAttempts.AttemptIndex = @attempt_index")
        bind("external_recurring_exchange_id").to(externalRecurringExchangeId)
        bind("date").to(date.toCloudDate())
        bind("step_index").to(stepIndex.toLong())
        bind("attempt_index").to(attemptIndex.toLong())
        appendClause("LIMIT 1")
      }
      .execute(transactionContext)
      .singleOrNull()
  }

  private fun TransactionScope.updateExchangeState(
    recurringExchangeId: Long,
    date: Date,
    state: Exchange.State
  ) {
    updateMutation("Exchanges") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("Date" to date.toCloudDate())
        set("State" to state)
      }
      .bufferTo(transactionContext)
  }

  private fun TransactionScope.updateExchangeStepAttempt(
    recurringExchangeId: Long,
    exchangeStepAttempt: ExchangeStepAttempt,
    state: ExchangeStepAttempt.State
  ): ExchangeStepAttempt {
    if (exchangeStepAttempt.state == state) {
      return exchangeStepAttempt
    }
    require(!exchangeStepAttempt.state.isTerminal) {
      "Attempt for Step: ${exchangeStepAttempt.stepIndex} is in a terminal state."
    }
    var logMessage = "Attempt for Step: ${exchangeStepAttempt.stepIndex} Failed."
    if (state == ExchangeStepAttempt.State.SUCCEEDED) {
      logMessage = "Attempt for Step: ${exchangeStepAttempt.stepIndex} Succeeded."
    }

    val updatedTime = Value.COMMIT_TIMESTAMP.toProto()
    val debugLog =
      ExchangeStepAttemptDetails.DebugLog.newBuilder()
        .apply {
          message = logMessage
          time = updatedTime
        }
        .build()
    val details =
      exchangeStepAttempt
        .details
        .toBuilder()
        .apply {
          updateTime = updatedTime
          addDebugLogEntries(debugLog)
        }
        .build()

    updateMutation("ExchangeStepAttempts") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("Date" to exchangeStepAttempt.date.toCloudDate())
        set("StepIndex" to exchangeStepAttempt.stepIndex.toLong())
        set("AttemptIndex" to exchangeStepAttempt.attemptNumber.toLong())
        set("State" to state)
        set("ExchangeStepAttemptDetails" to details)
        setJson("ExchangeStepAttemptDetailsJson" to details)
      }
      .bufferTo(transactionContext)

    return exchangeStepAttempt.toBuilder().setState(state).setDetails(details).build()
  }
}

private val ExchangeStepAttempt.State.isTerminal: Boolean
  get() =
    when (this) {
      ExchangeStepAttempt.State.ACTIVE -> false
      ExchangeStepAttempt.State.SUCCEEDED,
      ExchangeStepAttempt.State.FAILED,
      ExchangeStepAttempt.State.FAILED_STEP,
      ExchangeStepAttempt.State.UNRECOGNIZED,
      ExchangeStepAttempt.State.STATE_UNSPECIFIED -> true
    }
