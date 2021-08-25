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
import com.google.common.base.Optional
import com.google.type.Date
import java.time.Duration
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.makeStatement
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.exchangeStepAttemptDetails
import org.wfanet.measurement.kingdom.db.getExchangeStepFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.GetExchangeStep
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ClaimReadyExchangeStep.Result

private val DEFAULT_EXPIRATION_DURATION: Duration = Duration.ofDays(1)

class ClaimReadyExchangeStep(externalModelProviderId: Long?, externalDataProviderId: Long?) :
  SimpleSpannerWriter<Optional<Result>>() {
  data class Result(val step: ExchangeStep, val attemptIndex: Int)

  private val externalModelProviderIds =
    if (externalModelProviderId == null) emptyList()
    else listOf(ExternalId(externalModelProviderId))
  private val externalDataProviderIds =
    if (externalDataProviderId == null) emptyList() else listOf(ExternalId(externalDataProviderId))

  override suspend fun TransactionScope.runTransaction(): Optional<Result> {
    // Get the first ExchangeStep with status: READY | READY_FOR_RETRY  by given Provider id.
    val exchangeStepResult =
      GetExchangeStep(
          getExchangeStepFilter(
            externalModelProviderIds = externalModelProviderIds,
            externalDataProviderIds = externalDataProviderIds,
            states = listOf(ExchangeStep.State.READY_FOR_RETRY, ExchangeStep.State.READY)
          )
        )
        .execute(transactionContext)
        .singleOrNull()
        ?: return Optional.absent()

    val exchangeStep = exchangeStepResult.exchangeStep
    val recurringExchangeId = exchangeStepResult.recurringExchangeId

    // Create an Exchange Step Attempt for this Step.
    val attemptIndex =
      createExchangeStepAttempt(
        recurringExchangeId = recurringExchangeId,
        date = exchangeStep.date,
        stepIndex = exchangeStep.stepIndex.toLong()
      )

    // Finally, update the Exchange Step status to IN_PROGRESS.
    val updatedStep =
      updateExchangeStepState(
        exchangeStep = exchangeStep,
        recurringExchangeId = exchangeStepResult.recurringExchangeId,
        state = ExchangeStep.State.IN_PROGRESS
      )

    return Optional.of(Result(updatedStep, attemptIndex.toInt()))
  }

  private suspend fun TransactionScope.createExchangeStepAttempt(
    recurringExchangeId: Long,
    date: Date,
    stepIndex: Long
  ): Long {
    val now = clock.instant()

    val details = exchangeStepAttemptDetails {
      startTime = now.toProtoTime()
      // TODO(@yunyeng): Set ExchangeStepAttemptDetails with suitable fields.
    }

    val attemptIndex = findAttemptIndex(recurringExchangeId, date, stepIndex)

    transactionContext.bufferInsertMutation("ExchangeStepAttempts") {
      set("RecurringExchangeId" to recurringExchangeId)
      set("Date" to date.toCloudDate())
      set("StepIndex" to stepIndex)
      set("AttemptIndex" to attemptIndex)
      set("State" to ExchangeStepAttempt.State.ACTIVE)

      // TODO(@efoxepstein): make this variable based on the step type or something.
      set("ExpirationTime" to (now + DEFAULT_EXPIRATION_DURATION).toGcloudTimestamp())

      set("ExchangeStepAttemptDetails" to details)
      setJson("ExchangeStepAttemptDetailsJson" to details)
    }

    return attemptIndex
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
      makeStatement(sql) {
        bind("recurring_exchange_id").to(recurringExchangeId)
        bind("date").to(date.toCloudDate())
        bind("step_index").to(stepIndex)
      }

    val row: Struct = transactionContext.executeQuery(statement).single()

    return row.getLong("MaxAttemptIndex") + 1L
  }
}
