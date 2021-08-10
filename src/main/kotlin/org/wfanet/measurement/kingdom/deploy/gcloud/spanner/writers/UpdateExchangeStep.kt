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
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow

internal fun SpannerWriter.TransactionScope.updateExchangeStepsToReady(
  steps: List<ExchangeWorkflow.Step>,
  recurringExchangeId: Long,
  date: Date
) {
  for (step in steps) {
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

internal fun SpannerWriter.TransactionScope.updateExchangeStepState(
  exchangeStep: ExchangeStep,
  recurringExchangeId: Long,
  state: ExchangeStep.State
): ExchangeStep {
  if (exchangeStep.state == state) {
    return exchangeStep
  }
  require(!exchangeStep.state.isTerminal) {
    "ExchangeStep with StepIndex: ${exchangeStep.stepIndex} is in a terminal state."
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

internal val ExchangeStep.State.isTerminal: Boolean
  get() =
    when (this) {
      ExchangeStep.State.BLOCKED,
      ExchangeStep.State.READY,
      ExchangeStep.State.READY_FOR_RETRY,
      ExchangeStep.State.IN_PROGRESS -> false
      ExchangeStep.State.SUCCEEDED,
      ExchangeStep.State.FAILED,
      ExchangeStep.State.UNRECOGNIZED,
      ExchangeStep.State.STATE_UNSPECIFIED -> true
    }
