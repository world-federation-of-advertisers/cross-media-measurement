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
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.updateMutation
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.kingdom.db.getExchangeStepFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.GetExchangeStep
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepReader

class UpdateExchangeStepState(
  private val recurringExchangeId: Long,
  private val date: Date,
  private val stepIndex: Long,
  private val state: ExchangeStep.State
) : SpannerWriter<ExchangeStep, ExchangeStep>() {
  override suspend fun TransactionScope.runTransaction(): ExchangeStep {
    val filter =
      getExchangeStepFilter(
        recurringExchangeId = recurringExchangeId,
        date = date,
        stepIndex = stepIndex
      )
    // TODO: combine all updates into a single read and then multiple writes.
    val result = GetExchangeStep(filter).executeSingle(transactionContext)

    return updateExchangeStepState(result, state)
  }

  override fun ResultScope<ExchangeStep>.buildResult(): ExchangeStep {
    return checkNotNull(transactionResult)
      .toBuilder()
      .setUpdateTime(commitTimestamp.toProto())
      .build()
  }
}

internal fun SpannerWriter.TransactionScope.updateExchangeStepState(
  result: ExchangeStepReader.Result,
  state: ExchangeStep.State
): ExchangeStep {
  val exchangeStep = result.exchangeStep

  if (exchangeStep.state == state) {
    return exchangeStep
  }

  require(!exchangeStep.state.isTerminal) { "ExchangeStep: $exchangeStep is in a terminal state." }

  updateMutation("ExchangeSteps") {
      set("RecurringExchangeId" to result.recurringExchangeId)
      set("Date" to exchangeStep.date.toCloudDate())
      set("StepIndex" to exchangeStep.stepIndex.toLong())
      set("State" to state)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("ModelProviderId" to result.modelProviderId)
      set("DataProviderId" to result.dataProviderId)
    }
    .bufferTo(transactionContext)

  return exchangeStep.toBuilder().setState(state).build()
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
