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

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.type.Date
import kotlinx.coroutines.flow.single
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.getNullableLong
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetails

class CreateExchangeStepAttempt(
  private val recurringExchangeId: Long,
  private val externalRecurringExchangeId: ExternalId,
  private val date: Date,
  private val stepIndex: Long
) : SimpleSpannerWriter<ExchangeStepAttempt>() {
  override suspend fun TransactionScope.runTransaction(): ExchangeStepAttempt {
    // TODO: Set ExchangeStepAttemptDetails with suitable fields.
    val details = ExchangeStepAttemptDetails.getDefaultInstance()

    val exchangeStepAttempt =
      ExchangeStepAttempt.newBuilder()
        .also {
          it.date = date
          it.details = details
          it.state = ExchangeStepAttempt.State.ACTIVE
          it.stepIndex = stepIndex.toInt()
          it.attemptNumber = findAttemptIndex().toInt()
          it.externalRecurringExchangeId = externalRecurringExchangeId.value
        }
        .build()

    exchangeStepAttempt.toInsertMutation().bufferTo(transactionContext)

    return exchangeStepAttempt
  }

  private suspend fun TransactionScope.findAttemptIndex(): Long {
    val sql =
      """
      SELECT MAX(AttemptIndex) AS MaxAttemptIndex
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

    return (row.getNullableLong("MaxAttemptIndex") ?: 0L) + 1L
  }

  private fun ExchangeStepAttempt.toInsertMutation(): Mutation {
    return insertMutation("ExchangeStepAttempts") {
      set("RecurringExchangeId" to recurringExchangeId)
      set("Date" to date.toCloudDate())
      set("StepIndex" to stepIndex.toLong())
      set("AttemptIndex" to attemptNumber.toLong())
      set("ExchangeStepAttemptDetails" to details)
      setJson("ExchangeStepAttemptDetailsJson" to details)
    }
  }
}
