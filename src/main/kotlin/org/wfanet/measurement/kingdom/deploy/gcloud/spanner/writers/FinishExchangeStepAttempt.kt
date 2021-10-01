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
import com.google.type.Date
import io.grpc.Status
import java.time.Clock
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toSet
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetailsKt.debugLog
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.FinishExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.db.getExchangeStepFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.externalDataProviderId
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.externalModelProviderId
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.GetExchangeStep
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepAttemptReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RecurringExchangeReader

class FinishExchangeStepAttempt(
  private val request: FinishExchangeStepAttemptRequest,
  private val clock: Clock
) : SimpleSpannerWriter<ExchangeStepAttempt>() {
  private val externalModelProviderIds = listOfNotNull(request.provider.externalModelProviderId)
  private val externalDataProviderIds = listOfNotNull(request.provider.externalDataProviderId)
  private val externalRecurringExchangeId = request.externalRecurringExchangeId
  private val reqDate = request.date
  private val reqStepIndex = request.stepIndex
  private val reqAttemptIndex = request.attemptNumber

  override suspend fun TransactionScope.runTransaction(): ExchangeStepAttempt {
    val stepAttemptResult =
      getExchangeStepAttempt(
        externalRecurringExchangeId = externalRecurringExchangeId,
        date = reqDate,
        stepIndex = reqStepIndex,
        attemptIndex = reqAttemptIndex,
      )

    // TODO: the above and below reads should be consolidated into a single query.
    val stepResult =
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
        ?: failGrpc(Status.NOT_FOUND) { "ExchangeStepAttempt not found" }
    // TODO: use a KingdomInternalException above

    // TODO(yunyeng): Think about an ACTIVE case for auto-fail scenario.
    // See https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/190.
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (request.state) {
      ExchangeStepAttempt.State.SUCCEEDED -> succeed(stepResult, stepAttemptResult)
      ExchangeStepAttempt.State.FAILED -> temporarilyFail(stepResult, stepAttemptResult)
      ExchangeStepAttempt.State.FAILED_STEP -> permanentlyFail(stepResult, stepAttemptResult)
      ExchangeStepAttempt.State.ACTIVE -> failGrpc { "Request state ACTIVE is not terminal" }
      ExchangeStepAttempt.State.STATE_UNSPECIFIED, ExchangeStepAttempt.State.UNRECOGNIZED ->
        failGrpc { "Invalid request state" }
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
        .readByExternalRecurringExchangeId(
          transactionContext,
          ExternalId(externalRecurringExchangeId)
        )
        ?.recurringExchange
        ?.details
        ?.exchangeWorkflow
        ?: failGrpc(Status.NOT_FOUND) { "RecurringExchange not found" }
    // TODO: use a KingdomInternalException above

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

    if (allStepsCompleted(recurringExchangeId = recurringExchangeId, date = reqDate)) {
      updateExchangeState(
        recurringExchangeId = recurringExchangeId,
        date = exchangeStep.date,
        state = Exchange.State.SUCCEEDED
      )
    }

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
      SELECT ExchangeSteps.StepIndex
      FROM ExchangeSteps
      WHERE ExchangeSteps.RecurringExchangeId = @recurring_exchange_id
        AND ExchangeSteps.Date = @date
        AND ExchangeSteps.State = @state
      ORDER BY ExchangeSteps.StepIndex
      """.trimIndent()
    val statement: Statement =
      statement(sql) {
        bind("recurring_exchange_id").to(recurringExchangeId)
        bind("date").to(date.toCloudDate())
        bind("state").toProtoEnum(ExchangeStep.State.SUCCEEDED)
      }
    return transactionContext
      .executeQuery(statement)
      .map { it.getLong("StepIndex").toInt() }
      .toSet()
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
  ): ExchangeStepAttemptReader.Result {
    return ExchangeStepAttemptReader()
      .fillStatementBuilder {
        appendClause(
          """
          WHERE RecurringExchanges.ExternalRecurringExchangeId = @external_recurring_exchange_id
            AND ExchangeStepAttempts.Date = @date
            AND ExchangeStepAttempts.StepIndex = @step_index
            AND ExchangeStepAttempts.AttemptIndex = @attempt_index
          """.trimIndent()
        )
        bind("external_recurring_exchange_id").to(externalRecurringExchangeId)
        bind("date").to(date.toCloudDate())
        bind("step_index").to(stepIndex.toLong())
        bind("attempt_index").to(attemptIndex.toLong())
        appendClause("LIMIT 1")
      }
      .execute(transactionContext)
      .singleOrNull()
      ?: failGrpc(Status.NOT_FOUND) { "ExchangeStepAttempt not found" }
    // TODO: use a KingdomInternalException above
  }

  private fun TransactionScope.updateExchangeState(
    recurringExchangeId: Long,
    date: Date,
    state: Exchange.State
  ) {
    transactionContext.bufferUpdateMutation("Exchanges") {
      set("RecurringExchangeId" to recurringExchangeId)
      set("Date" to date.toCloudDate())
      set("State" to state)
    }
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

    val now = clock.instant().toProtoTime()
    val details =
      exchangeStepAttempt.details.copy {
        updateTime = now
        debugLogEntries +=
          debugLog {
            message = logMessage
            time = now
          }
      }

    transactionContext.bufferUpdateMutation("ExchangeStepAttempts") {
      set("RecurringExchangeId" to recurringExchangeId)
      set("Date" to exchangeStepAttempt.date.toCloudDate())
      set("StepIndex" to exchangeStepAttempt.stepIndex.toLong())
      set("AttemptIndex" to exchangeStepAttempt.attemptNumber.toLong())
      set("State" to state)
      set("ExchangeStepAttemptDetails" to details)
      setJson("ExchangeStepAttemptDetailsJson" to details)
    }

    return exchangeStepAttempt.copy {
      this.state = state
      this.details = details
    }
  }

  private suspend fun TransactionScope.allStepsCompleted(
    recurringExchangeId: Long,
    date: Date
  ): Boolean {
    val sql =
      """
      SELECT COUNT(*) AS NumberOfIncomplete
      FROM ExchangeSteps
      WHERE ExchangeSteps.RecurringExchangeId = @recurring_exchange_id
        AND ExchangeSteps.Date = @date
        AND ExchangeSteps.State != @state
      """.trimIndent()

    val statement: Statement =
      statement(sql) {
        bind("recurring_exchange_id" to recurringExchangeId)
        bind("date" to date.toCloudDate())
        bind("state" to ExchangeStep.State.SUCCEEDED)
      }
    val row: Struct = transactionContext.executeQuery(statement).single()

    // Since the current step is still IN_PROGRESS, if this is 1 then after updating the current
    // step, there will be no more incomplete steps.
    return row.getLong("NumberOfIncomplete") == 1L
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
