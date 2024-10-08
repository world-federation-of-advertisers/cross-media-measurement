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

import com.google.cloud.spanner.Value
import com.google.type.Date
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeDetails
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RecurringExchangeReader

class CreateExchangesAndSteps(
  private val party: ExchangeWorkflow.Party,
  private val externalPartyId: ExternalId,
) : SimpleSpannerWriter<Unit>() {

  override suspend fun TransactionScope.runTransaction() {
    val recurringExchangeResult: RecurringExchangeReader.Result = getRecurringExchange() ?: return

    val recurringExchange = recurringExchangeResult.recurringExchange
    val recurringExchangeId = recurringExchangeResult.recurringExchangeId
    val modelProviderId = recurringExchangeResult.modelProviderId
    val dataProviderId = recurringExchangeResult.dataProviderId
    val nextExchangeDate = recurringExchange.nextExchangeDate
    val workflow = recurringExchange.details.exchangeWorkflow

    // Create the Exchange with the date equal to next exchange date.
    createExchange(recurringExchangeId = recurringExchangeId, date = nextExchangeDate)

    // Calculate the new next exchange date for the recurring exchange.
    val nextNextExchangeDate =
      nextExchangeDate.applyCronSchedule(recurringExchange.details.cronSchedule)

    // Update recurring exchange with new next exchange date. Its state doesn't change.
    updateRecurringExchange(
      recurringExchangeId = recurringExchangeId,
      nextExchangeDate = nextNextExchangeDate,
    )

    // Create all steps for the Exchange, set them all to BLOCKED State initially.
    createExchangeSteps(
      workflow = workflow,
      recurringExchangeId = recurringExchangeId,
      date = nextExchangeDate,
      modelProviderId = modelProviderId,
      dataProviderId = dataProviderId,
    )

    // Update all Steps States based on the workflow.
    updateExchangeStepsToReady(
      steps = workflow.stepsList.filter { step -> step.prerequisiteStepIndicesCount == 0 },
      recurringExchangeId = recurringExchangeId,
      date = nextExchangeDate,
    )
  }

  private suspend fun TransactionScope.getRecurringExchange(): RecurringExchangeReader.Result? {
    return RecurringExchangeReader()
      .fillStatementBuilder {
        val conjuncts =
          mutableListOf(
            "State = @${Params.RECURRING_EXCHANGE_STATE}",
            "NextExchangeDate <= CURRENT_DATE('+0')",
            """
            @${Params.EXCHANGE_STATE} NOT IN (
              SELECT Exchanges.State
              FROM Exchanges
              WHERE Exchanges.RecurringExchangeId = RecurringExchanges.RecurringExchangeId
              ORDER BY Exchanges.Date DESC
              LIMIT 1
            )
            """
              .trimIndent(),
          )
        bind(Params.RECURRING_EXCHANGE_STATE).toInt64(RecurringExchange.State.ACTIVE)
        bind(Params.EXCHANGE_STATE).toInt64(Exchange.State.FAILED)

        when (party) {
          ExchangeWorkflow.Party.MODEL_PROVIDER -> {
            conjuncts.add("ModelProviders.ExternalModelProviderId = @${Params.EXTERNAL_PARTY_ID}")
          }
          ExchangeWorkflow.Party.DATA_PROVIDER -> {
            conjuncts.add("DataProviders.ExternalDataProviderId = @${Params.EXTERNAL_PARTY_ID}")
          }
          ExchangeWorkflow.Party.PARTY_UNSPECIFIED,
          ExchangeWorkflow.Party.UNRECOGNIZED -> error("Invalid party $party")
        }
        bind(Params.EXTERNAL_PARTY_ID to externalPartyId)

        appendClause("WHERE")
        append(conjuncts.joinToString(" AND "))
        appendClause("ORDER BY NextExchangeDate")
        appendClause("LIMIT 1")
      }
      .execute(transactionContext)
      .singleOrNull()
  }

  private fun TransactionScope.createExchange(recurringExchangeId: Long, date: Date) {
    // TODO: Set ExchangeDetails with proper Audit trail hash.
    val exchangeDetails = ExchangeDetails.getDefaultInstance()
    transactionContext.bufferInsertMutation("Exchanges") {
      set("RecurringExchangeId" to recurringExchangeId)
      set("Date" to date.toCloudDate())
      set("State").toInt64(Exchange.State.ACTIVE)
      set("ExchangeDetails").to(exchangeDetails)
    }
  }

  private fun TransactionScope.updateRecurringExchange(
    recurringExchangeId: Long,
    nextExchangeDate: Date,
  ) {
    transactionContext.bufferUpdateMutation("RecurringExchanges") {
      set("RecurringExchangeId" to recurringExchangeId)
      set("NextExchangeDate" to nextExchangeDate.toCloudDate())
    }
  }

  private fun TransactionScope.createExchangeSteps(
    workflow: ExchangeWorkflow,
    recurringExchangeId: Long,
    date: Date,
    modelProviderId: Long,
    dataProviderId: Long,
  ) {
    for (step in workflow.stepsList) {
      transactionContext.bufferInsertMutation("ExchangeSteps") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("Date" to date.toCloudDate())
        set("StepIndex" to step.stepIndex.toLong())
        set("State").toInt64(ExchangeStep.State.BLOCKED)
        set("UpdateTime" to Value.COMMIT_TIMESTAMP)
        set(
          "ModelProviderId" to
            if (step.party == ExchangeWorkflow.Party.MODEL_PROVIDER) modelProviderId else null
        )
        set(
          "DataProviderId" to
            if (step.party == ExchangeWorkflow.Party.DATA_PROVIDER) dataProviderId else null
        )
      }
    }
  }

  // TODO: Decide on the format for cronSchedule.
  // See https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/180.
  private fun Date.applyCronSchedule(cronSchedule: String): Date {
    return when (cronSchedule) {
      "@daily" -> this.toLocalDate().plusDays(1).toProtoDate()
      "@weekly" -> this.toLocalDate().plusWeeks(1).toProtoDate()
      "@monthly" -> this.toLocalDate().plusMonths(1).toProtoDate()
      "@yearly" -> this.toLocalDate().plusYears(1).toProtoDate()
      else -> error("Cannot support this.")
    }
  }

  private object Params {
    const val RECURRING_EXCHANGE_STATE = "recurringExchangeState"
    const val EXCHANGE_STATE = "exchangeState"
    const val EXTERNAL_PARTY_ID = "externalPartyId"
  }
}
