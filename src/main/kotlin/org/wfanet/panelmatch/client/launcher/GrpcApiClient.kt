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
import kotlinx.coroutines.sync.Semaphore
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey as V2AlphaExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt as V2AlphaExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKt.debugLogEntry
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow as V2AlphaExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.RecurringExchangeParentKey
import org.wfanet.measurement.api.v2alpha.appendExchangeStepAttemptLogEntryRequest
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.finishExchangeStepAttemptRequest
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.common.Fingerprinters.farmHashFingerprint64
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.common.toInternal
import org.wfanet.panelmatch.client.internal.ExchangeStepAttempt
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party
import org.wfanet.panelmatch.client.launcher.ApiClient.ClaimedExchangeStep

class GrpcApiClient(
  identity: Identity,
  private val exchangeStepsClient: ExchangeStepsCoroutineStub,
  private val exchangeStepAttemptsClient: ExchangeStepAttemptsCoroutineStub,
  private val clock: Clock = Clock.systemUTC(),
  maxClaimedExchangeSteps: Int? = null,
) : ApiClient {
  private val semaphore = maxClaimedExchangeSteps?.let { maxSteps -> Semaphore(maxSteps) }

  private val recurringExchangeParentKey: RecurringExchangeParentKey =
    when (identity.party) {
      Party.DATA_PROVIDER -> DataProviderKey(identity.id)
      Party.MODEL_PROVIDER -> ModelProviderKey(identity.id)
      Party.PARTY_UNSPECIFIED,
      Party.UNRECOGNIZED -> throw IllegalArgumentException("Unsupported party ${identity.party}")
    }

  override suspend fun claimExchangeStep(): ClaimedExchangeStep? {
    if (semaphore != null) {
      val semaphoreAcquired = semaphore.tryAcquire()
      if (!semaphoreAcquired) return null
    }
    val response: ClaimReadyExchangeStepResponse =
      exchangeStepsClient.claimReadyExchangeStep(
        claimReadyExchangeStepRequest { parent = recurringExchangeParentKey.toName() }
      )
    if (response.hasExchangeStep()) {
      val v2AlphaAttemptKey =
        requireNotNull(V2AlphaExchangeStepAttemptKey.fromName(response.exchangeStepAttempt))
      val exchangeStep = response.exchangeStep
      val v2AlphaExchangeWorkflow = exchangeStep.exchangeWorkflow.unpack<V2AlphaExchangeWorkflow>()
      return ClaimedExchangeStep(
        attemptKey = v2AlphaAttemptKey.toInternal(),
        exchangeDate = exchangeStep.exchangeDate.toLocalDate(),
        stepIndex = exchangeStep.stepIndex,
        workflow = v2AlphaExchangeWorkflow.toInternal(),
        workflowFingerprint = v2AlphaExchangeWorkflow.farmHashFingerprint64(),
      )
    }
    semaphore?.release()
    return null
  }

  override suspend fun appendLogEntry(key: ExchangeStepAttemptKey, messages: Iterable<String>) {
    val request = appendExchangeStepAttemptLogEntryRequest {
      name = key.simpleName
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
    semaphore?.release()
    val request = finishExchangeStepAttemptRequest {
      name = key.simpleName
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

  private fun V2AlphaExchangeStepAttemptKey.toInternal(): ExchangeStepAttemptKey {
    return ExchangeStepAttemptKey(
      recurringExchangeId = recurringExchangeId,
      exchangeId = exchangeId,
      stepId = exchangeStepId,
      attemptId = exchangeStepAttemptId,
      simpleName = toName(),
    )
  }

  private fun ExchangeStepAttempt.State.toV2Alpha(): V2AlphaExchangeStepAttempt.State {
    return when (this) {
      ExchangeStepAttempt.State.IN_PROGRESS -> V2AlphaExchangeStepAttempt.State.ACTIVE
      ExchangeStepAttempt.State.SUCCEEDED -> V2AlphaExchangeStepAttempt.State.SUCCEEDED
      ExchangeStepAttempt.State.FAILED -> V2AlphaExchangeStepAttempt.State.FAILED
      ExchangeStepAttempt.State.FAILED_STEP -> V2AlphaExchangeStepAttempt.State.FAILED_STEP
      else -> throw IllegalArgumentException("Unsupported attempt state: $this")
    }
  }
}
