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

import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.type.Date
import java.time.Clock
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetails
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ExchangeStepAttemptNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ExchangeStepNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RecurringExchangeNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepAttemptReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RecurringExchangeReader

/**
 * [SpannerWriter] for finishing an [ExchangeStepAttempt].
 *
 * Throws one of the following [KingdomInternalException] types on [execute]:
 * * [ExchangeStepAttemptNotFoundException]
 * * [ExchangeStepNotFoundException]
 * * [RecurringExchangeNotFoundException]
 */
class FinishExchangeStepAttempt(
  private val externalRecurringExchangeId: ExternalId,
  private val exchangeDate: Date,
  private val stepIndex: Int,
  private val attemptNumber: Int,
  private val terminalState: ExchangeStepAttempt.State,
  private val debugLogEntries: Iterable<ExchangeStepAttemptDetails.DebugLog>,
  private val clock: Clock,
) : SimpleSpannerWriter<ExchangeStepAttempt>() {
  private lateinit var exchangeStepResult: ExchangeStepReader.Result
  private lateinit var exchangeStepAttemptResult: ExchangeStepAttemptReader.Result

  private val recurringExchangeId: InternalId
    get() = InternalId(exchangeStepAttemptResult.recurringExchangeId)

  private val exchangeStep: ExchangeStep
    get() = exchangeStepResult.exchangeStep

  private val exchangeStepAttempt: ExchangeStepAttempt
    get() = exchangeStepAttemptResult.exchangeStepAttempt

  override suspend fun TransactionScope.runTransaction(): ExchangeStepAttempt {
    exchangeStepAttemptResult = getExchangeStepAttempt()

    // TODO: the above and below reads should be consolidated into a single query.
    exchangeStepResult = readExchangeStepResult()

    // TODO(yunyeng): Think about an ACTIVE case for auto-fail scenario.
    // See https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/190.
    return when (terminalState) {
      ExchangeStepAttempt.State.SUCCEEDED -> succeed()
      ExchangeStepAttempt.State.FAILED -> temporarilyFail()
      ExchangeStepAttempt.State.FAILED_STEP -> permanentlyFail()
      ExchangeStepAttempt.State.ACTIVE,
      ExchangeStepAttempt.State.STATE_UNSPECIFIED,
      ExchangeStepAttempt.State.UNRECOGNIZED ->
        throw IllegalArgumentException("Invalid request state: $terminalState")
    }
  }

  private suspend fun TransactionScope.readExchangeStepResult(): ExchangeStepReader.Result {
    return ExchangeStepReader()
      .readByExternalIds(txn, externalRecurringExchangeId, exchangeDate, stepIndex)
      ?: throw ExchangeStepNotFoundException(externalRecurringExchangeId, exchangeDate, stepIndex)
  }

  private suspend fun TransactionScope.succeed(): ExchangeStepAttempt {
    updateExchangeStepState(
      exchangeStep = exchangeStep,
      recurringExchangeId = recurringExchangeId.value,
      state = ExchangeStep.State.SUCCEEDED,
    )

    val workflow =
      RecurringExchangeReader()
        .fillStatementBuilder {
          appendClause("WHERE RecurringExchangeId = @recurring_exchange_id")
          bind("recurring_exchange_id").to(recurringExchangeId.value)
        }
        .execute(transactionContext)
        .singleOrNull()
        ?.recurringExchange
        ?.details
        ?.exchangeWorkflow ?: throw RecurringExchangeNotFoundException(externalRecurringExchangeId)

    val steps = findNewlyUnblockedExchangeSteps(workflow)
    updateExchangeStepsToReady(
      steps,
      recurringExchangeId = recurringExchangeId.value,
      date = exchangeDate,
    )

    if (allStepsCompleted()) {
      updateExchangeState(Exchange.State.SUCCEEDED)
    }

    return updateExchangeStepAttempt(ExchangeStepAttempt.State.SUCCEEDED)
  }

  private fun TransactionScope.temporarilyFail(): ExchangeStepAttempt {
    updateExchangeStepState(
      exchangeStep = exchangeStep,
      recurringExchangeId = recurringExchangeId.value,
      state = ExchangeStep.State.READY_FOR_RETRY,
    )
    return updateExchangeStepAttempt(ExchangeStepAttempt.State.FAILED)
  }

  private fun TransactionScope.permanentlyFail(): ExchangeStepAttempt {
    updateExchangeStepState(
      exchangeStep = exchangeStep,
      recurringExchangeId = recurringExchangeId.value,
      state = ExchangeStep.State.FAILED,
    )
    updateExchangeState(Exchange.State.FAILED)
    return updateExchangeStepAttempt(ExchangeStepAttempt.State.FAILED_STEP)
  }

  private suspend fun TransactionScope.getExchangeStepStates(): Map<Int, ExchangeStep.State> {
    val sql =
      """
      SELECT ExchangeSteps.StepIndex, ExchangeSteps.State
      FROM ExchangeSteps
      WHERE ExchangeSteps.RecurringExchangeId = @recurring_exchange_id
        AND ExchangeSteps.Date = @date
      ORDER BY ExchangeSteps.StepIndex
      """
        .trimIndent()
    val statement: Statement =
      statement(sql) {
        bind("recurring_exchange_id" to recurringExchangeId.value)
        bind("date" to exchangeDate.toCloudDate())
      }
    return transactionContext
      .executeQuery(statement, Options.tag("writer=$writerName,action=readExchangeStepStates"))
      .toList()
      .associate {
        it.getLong("StepIndex").toInt() to it.getProtoEnum("State", ExchangeStep.State::forNumber)
      }
  }

  private suspend fun TransactionScope.findNewlyUnblockedExchangeSteps(
    workflow: ExchangeWorkflow
  ): List<ExchangeWorkflow.Step> {
    val stepStates = getExchangeStepStates()
    return workflow.stepsList.filter { step ->
      stepStates[step.stepIndex] == ExchangeStep.State.BLOCKED &&
        step.prerequisiteStepIndicesCount > 0 &&
        step.prerequisiteStepIndicesList.all {
          it == stepIndex || stepStates[it] == ExchangeStep.State.SUCCEEDED
        }
    }
  }

  private suspend fun TransactionScope.getExchangeStepAttempt(): ExchangeStepAttemptReader.Result {
    return ExchangeStepAttemptReader()
      .readByExternalIds(
        transactionContext,
        externalRecurringExchangeId,
        exchangeDate,
        stepIndex,
        attemptNumber,
      )
      ?: throw ExchangeStepAttemptNotFoundException(
        externalRecurringExchangeId,
        exchangeDate,
        stepIndex,
        attemptNumber,
      )
  }

  private fun TransactionScope.updateExchangeState(state: Exchange.State) {
    transactionContext.bufferUpdateMutation("Exchanges") {
      set("RecurringExchangeId" to recurringExchangeId)
      set("Date" to exchangeDate.toCloudDate())
      set("State").toInt64(state)
    }
  }

  private fun TransactionScope.updateExchangeStepAttempt(
    state: ExchangeStepAttempt.State
  ): ExchangeStepAttempt {
    if (exchangeStepAttemptResult.exchangeStepAttempt.state == state) {
      return exchangeStepAttemptResult.exchangeStepAttempt
    }
    require(!exchangeStepAttempt.state.isTerminal) {
      "Attempt for Step: ${exchangeStepAttempt.stepIndex} is in a terminal state."
    }

    val now = clock.instant().toProtoTime()
    val details =
      exchangeStepAttempt.details.copy {
        updateTime = now
        debugLogEntries += this@FinishExchangeStepAttempt.debugLogEntries
      }

    transactionContext.bufferUpdateMutation("ExchangeStepAttempts") {
      set("RecurringExchangeId" to recurringExchangeId)
      set("Date" to exchangeDate.toCloudDate())
      set("StepIndex" to stepIndex.toLong())
      set("AttemptIndex" to attemptNumber.toLong())
      set("State").toInt64(state)
      set("ExchangeStepAttemptDetails").to(details)
    }

    return exchangeStepAttempt.copy {
      this.state = state
      this.details = details
    }
  }

  private suspend fun TransactionScope.allStepsCompleted(): Boolean {
    val sql =
      """
      SELECT COUNT(*) AS NumberOfIncomplete
      FROM ExchangeSteps
      WHERE ExchangeSteps.RecurringExchangeId = @recurring_exchange_id
        AND ExchangeSteps.Date = @date
        AND ExchangeSteps.State != @state
      """
        .trimIndent()

    val statement: Statement =
      statement(sql) {
        bind("recurring_exchange_id" to recurringExchangeId)
        bind("date" to exchangeDate.toCloudDate())
        bind("state").toInt64(ExchangeStep.State.SUCCEEDED)
      }
    val row: Struct =
      transactionContext
        .executeQuery(statement, Options.tag("writer=$writerName,action=allStepsCompleted"))
        .single()

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
