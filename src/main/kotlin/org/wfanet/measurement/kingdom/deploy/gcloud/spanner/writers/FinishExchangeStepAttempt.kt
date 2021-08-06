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
import kotlinx.coroutines.flow.Flow
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
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepAttemptReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RecurringExchangeReader

class FinishExchangeStepAttempt(private val request: FinishExchangeStepAttemptRequest) :
  SimpleSpannerWriter<ExchangeStepAttempt>() {

  private val externalRecurringExchangeId = request.externalRecurringExchangeId
  private val reqDate = request.date
  private val reqStepIndex = request.stepIndex
  private val reqAttemptIndex = request.attemptNumber

  override suspend fun TransactionScope.runTransaction(): ExchangeStepAttempt {
    return when (request.state) {
      ExchangeStepAttempt.State.SUCCEEDED -> succeed()
      ExchangeStepAttempt.State.FAILED -> temporarilyFail()
      ExchangeStepAttempt.State.FAILED_STEP -> permanentlyFail()
      else -> ExchangeStepAttempt.getDefaultInstance()
    }
  }

  private suspend fun TransactionScope.succeed(): ExchangeStepAttempt {
    val stepAttemptResult =
      findStepAttempt(
        externalRecurringExchangeId = externalRecurringExchangeId,
        date = reqDate,
        stepIndex = reqStepIndex,
        attemptIndex = reqAttemptIndex,
      )
        ?: throw error("Attempt for Step: $reqStepIndex not found.")

    val exchangeStepAttempt = stepAttemptResult.exchangeStepAttempt
    val recurringExchangeId = stepAttemptResult.recurringExchangeId

    updateExchangeStepState(
      recurringExchangeId = recurringExchangeId,
      date = exchangeStepAttempt.date,
      stepIndex = exchangeStepAttempt.stepIndex,
      state = ExchangeStep.State.SUCCEEDED
    )

    // TODO(yunyeng): Update to externalExchangeWorkflow
    val workflow =
      RecurringExchangeReader()
        .readExternalId(transactionContext, ExternalId(externalRecurringExchangeId))
        .recurringExchange
        .details
        .exchangeWorkflow

    updateExchangeSteps(
      workflow = ExchangeWorkflow.getDefaultInstance(),
      recurringExchangeId = recurringExchangeId,
      date = reqDate
    )

    return updateExchangeStepAttempt(
      recurringExchangeId = recurringExchangeId,
      exchangeStepAttempt = exchangeStepAttempt,
      state = ExchangeStepAttempt.State.SUCCEEDED
    )
  }

  private suspend fun TransactionScope.temporarilyFail(): ExchangeStepAttempt {
    val stepAttemptResult =
      findStepAttempt(
        externalRecurringExchangeId = externalRecurringExchangeId,
        date = reqDate,
        stepIndex = reqStepIndex,
        attemptIndex = reqAttemptIndex,
      )
        ?: throw error("Attempt for Step: $reqStepIndex not found.")

    val exchangeStepAttempt = stepAttemptResult.exchangeStepAttempt
    val recurringExchangeId = stepAttemptResult.recurringExchangeId

    updateExchangeStepState(
      recurringExchangeId = recurringExchangeId,
      date = exchangeStepAttempt.date,
      stepIndex = exchangeStepAttempt.stepIndex,
      state = ExchangeStep.State.READY_FOR_RETRY
    )

    return updateExchangeStepAttempt(
      recurringExchangeId = recurringExchangeId,
      exchangeStepAttempt = exchangeStepAttempt,
      state = ExchangeStepAttempt.State.FAILED
    )
  }

  private suspend fun TransactionScope.permanentlyFail(): ExchangeStepAttempt {
    val stepAttemptResult =
      findStepAttempt(
        externalRecurringExchangeId = externalRecurringExchangeId,
        date = reqDate,
        stepIndex = reqStepIndex,
        attemptIndex = reqAttemptIndex,
      )
        ?: throw error("Attempt for Step: $reqStepIndex not found.")

    val exchangeStepAttempt = stepAttemptResult.exchangeStepAttempt
    val recurringExchangeId = stepAttemptResult.recurringExchangeId

    updateExchangeStepState(
      recurringExchangeId = recurringExchangeId,
      date = exchangeStepAttempt.date,
      stepIndex = exchangeStepAttempt.stepIndex,
      state = ExchangeStep.State.FAILED
    )

    var exchangeFailed = true
    getAllExchangeSteps(recurringExchangeId = recurringExchangeId, date = reqDate).collect {
      if (!it.exchangeStep.state.isTerminal) {
        exchangeFailed = false
      }
    }

    if (exchangeFailed) {
      updateExchangeState(
        recurringExchangeId = recurringExchangeId,
        date = exchangeStepAttempt.date,
        state = Exchange.State.FAILED
      )
    }

    return updateExchangeStepAttempt(
      recurringExchangeId = recurringExchangeId,
      exchangeStepAttempt = exchangeStepAttempt,
      state = ExchangeStepAttempt.State.FAILED_STEP
    )
  }

  private suspend fun TransactionScope.updateExchangeSteps(
    workflow: ExchangeWorkflow,
    recurringExchangeId: Long,
    date: Date
  ) {
    val completedSteps = getCompletedSteps(recurringExchangeId, date)
    for (step in findReadyExchangeSteps(workflow, completedSteps)) {
      updateMutation("ExchangeSteps") {
          set("RecurringExchangeId" to recurringExchangeId)
          set("Date" to date.toCloudDate())
          set("StepIndex" to step.stepIndex.toLong())
          set("State" to ExchangeStep.State.READY)
          set("UpdateTime" to Value.COMMIT_TIMESTAMP)
        }
        .bufferTo(transactionContext)
    }
  }

  private suspend fun TransactionScope.findStepAttempt(
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

  private fun findReadyExchangeSteps(
    workflow: ExchangeWorkflow,
    completedStepIndexes: Set<Int>
  ): List<ExchangeWorkflow.Step> {
    return workflow.stepsList.filter { step ->
      step.prerequisiteStepIndicesCount == 0 ||
        step.prerequisiteStepIndicesList.all { it in completedStepIndexes }
    }
  }

  private suspend fun TransactionScope.getCompletedSteps(
    recurringExchangeId: Long,
    date: Date
  ): Set<Int> {
    val sql =
      """
      SELECT ExchangeSteps.StepIndex
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

  private fun TransactionScope.getAllExchangeSteps(
    recurringExchangeId: Long,
    date: Date
  ): Flow<ExchangeStepReader.Result> {
    return ExchangeStepReader()
      .withBuilder {
        appendClause("WHERE ExchangeSteps.RecurringExchangeId = @recurring_exchange_id")
        appendClause("AND ExchangeSteps.Date = @date")
        bind("recurring_exchange_id").to(recurringExchangeId)
        bind("date").to(date.toCloudDate())

        appendClause("ORDER BY ExchangeSteps.StepIndex")
      }
      .execute(transactionContext)
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

  private fun TransactionScope.updateExchangeStepState(
    recurringExchangeId: Long,
    date: Date,
    stepIndex: Int,
    state: ExchangeStep.State
  ) {
    updateMutation("ExchangeSteps") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("Date" to date.toCloudDate())
        set("StepIndex" to stepIndex.toLong())
        set("State" to state)
        set("UpdateTime" to Value.COMMIT_TIMESTAMP)
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

    val updatedTime = Value.COMMIT_TIMESTAMP.toProto()
    val debugLog =
      ExchangeStepAttemptDetails.DebugLog.newBuilder()
        .apply {
          message = "Attempt for Step: ${exchangeStepAttempt.stepIndex} Failed."
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

    return exchangeStepAttempt.toBuilder().setDetails(details).build()
  }
}

private val ExchangeStep.State.isTerminal: Boolean
  get() =
    when (this) {
      ExchangeStep.State.READY,
      ExchangeStep.State.READY_FOR_RETRY,
      ExchangeStep.State.SUCCEEDED,
      ExchangeStep.State.IN_PROGRESS -> false
      ExchangeStep.State.BLOCKED,
      ExchangeStep.State.FAILED,
      ExchangeStep.State.UNRECOGNIZED,
      ExchangeStep.State.STATE_UNSPECIFIED -> true
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
