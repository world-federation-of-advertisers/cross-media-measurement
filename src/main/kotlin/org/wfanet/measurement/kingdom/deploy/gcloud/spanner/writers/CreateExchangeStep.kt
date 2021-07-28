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
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.ExchangeStep

class CreateExchangeStep(
  private val exchangeStep: ExchangeStep,
  private val recurringExchangeId: Long,
  private val modelProviderId: Long? = null,
  private val dataProviderId: Long? = null,
) : SpannerWriter<ExchangeStep, ExchangeStep>() {
  override suspend fun TransactionScope.runTransaction(): ExchangeStep {
    insertMutation("ExchangeSteps") {
      set("RecurringExchangeId" to recurringExchangeId)
      set("Date" to exchangeStep.date.toCloudDate())
      set("StepIndex" to exchangeStep.stepIndex.toLong())
      set("State" to exchangeStep.state)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("ModelProviderId" to modelProviderId)
      set("DataProviderId" to dataProviderId)
    }.bufferTo(transactionContext)

    return exchangeStep
  }

  override fun ResultScope<ExchangeStep>.buildResult(): ExchangeStep {
    return checkNotNull(transactionResult)
      .toBuilder()
      .apply { updateTime = commitTimestamp.toProto() }
      .build()
  }
}
