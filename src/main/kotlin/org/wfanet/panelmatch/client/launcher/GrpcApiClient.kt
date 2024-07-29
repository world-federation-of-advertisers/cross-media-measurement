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

package org.wfanet.panelmatch.client.launcher

import com.google.protobuf.kotlin.unpack
import java.time.Clock
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt as V2AlphaExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKt.debugLogEntry
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.RecurringExchangeParentKey
import org.wfanet.measurement.api.v2alpha.appendExchangeStepAttemptLogEntryRequest
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.finishExchangeStepAttemptRequest
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.common.toInternal
import org.wfanet.panelmatch.client.internal.ExchangeStepAttempt
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party
import org.wfanet.panelmatch.client.launcher.ApiClient.ClaimedExchangeStep
import org.wfanet.panelmatch.common.Fingerprinters.sha256

class GrpcApiClient(
  identity: Identity,
  private val exchangeStepsClient: ExchangeStepsCoroutineStub,
  private val exchangeStepAttemptsClient: ExchangeStepAttemptsCoroutineStub,
  private val clock: Clock = Clock.systemUTC(),
) : ApiClient {

  private val recurringExchangeParentKey: RecurringExchangeParentKey =
    when (identity.party) {
      Party.DATA_PROVIDER -> DataProviderKey(identity.id)
      Party.MODEL_PROVIDER -> ModelProviderKey(identity.id)
      Party.PARTY_UNSPECIFIED,
      Party.UNRECOGNIZED -> throw IllegalArgumentException("Unsupported party ${identity.party}")
    }

  override suspend fun claimExchangeStep(): ClaimedExchangeStep? {
    val response: ClaimReadyExchangeStepResponse =
      exchangeStepsClient.claimReadyExchangeStep(
        claimReadyExchangeStepRequest { parent = recurringExchangeParentKey.toName() }
      )
    if (!response.hasExchangeStep()) {
      return null
    }

    val v2AlphaAttemptKey =
      requireNotNull(CanonicalExchangeStepAttemptKey.fromName(response.exchangeStepAttempt))
    val exchangeStep = response.exchangeStep
    val packedV2AlphaWorkflow = exchangeStep.exchangeWorkflow
    return ClaimedExchangeStep(
      attemptKey = v2AlphaAttemptKey.toInternal(),
      exchangeDate = exchangeStep.exchangeDate.toLocalDate(),
      stepIndex = exchangeStep.stepIndex,
      workflow = packedV2AlphaWorkflow.unpack<ExchangeWorkflow>().toInternal(),
      workflowFingerprint = sha256(packedV2AlphaWorkflow.value),
    )
  }

  override suspend fun appendLogEntry(key: ExchangeStepAttemptKey, messages: Iterable<String>) {
    val request = appendExchangeStepAttemptLogEntryRequest {
      name = key.toResourceName()
      for (message in messages) {
        logEntries += makeLogEntry(message)
      }
    }
    exchangeStepAttemptsClient.appendExchangeStepAttemptLogEntry(request)
  }

  override suspend fun finishExchangeStepAttempt(
    key: ExchangeStepAttemptKey,
    finalState: ExchangeStepAttempt.State,
    logEntryMessages: Iterable<String>,
  ) {
    val request = finishExchangeStepAttemptRequest {
      name = key.toResourceName()
      this.finalState = finalState.toV2Alpha()
      for (message in logEntryMessages) {
        logEntries += makeLogEntry(message)
      }
    }
    exchangeStepAttemptsClient.finishExchangeStepAttempt(request)
  }

  private fun makeLogEntry(message: String): V2AlphaExchangeStepAttempt.DebugLogEntry {
    return debugLogEntry {
      entryTime = clock.instant().toProtoTime()
      this.message = message
    }
  }

  private fun CanonicalExchangeStepAttemptKey.toInternal(): ExchangeStepAttemptKey {
    return ExchangeStepAttemptKey(
      recurringExchangeId = recurringExchangeId,
      exchangeId = exchangeId,
      stepId = exchangeStepId,
      attemptId = exchangeStepAttemptId,
    )
  }

  private fun ExchangeStepAttemptKey.toResourceName(): String {
    return CanonicalExchangeStepAttemptKey(
        recurringExchangeId = recurringExchangeId,
        exchangeId = exchangeId,
        exchangeStepId = stepId,
        exchangeStepAttemptId = attemptId,
      )
      .toName()
  }

  private fun ExchangeStepAttempt.State.toV2Alpha(): V2AlphaExchangeStepAttempt.State {
    return when (this) {
      ExchangeStepAttempt.State.IN_PROGRESS -> V2AlphaExchangeStepAttempt.State.ACTIVE
      ExchangeStepAttempt.State.SUCCEEDED -> V2AlphaExchangeStepAttempt.State.SUCCEEDED
      ExchangeStepAttempt.State.FAILED -> V2AlphaExchangeStepAttempt.State.FAILED
      ExchangeStepAttempt.State.FAILED_STEP -> V2AlphaExchangeStepAttempt.State.FAILED_STEP
      ExchangeStepAttempt.State.STATE_UNSPECIFIED,
      ExchangeStepAttempt.State.UNRECOGNIZED ->
        throw IllegalArgumentException("Unsupported attempt state: $this")
    }
  }
}
