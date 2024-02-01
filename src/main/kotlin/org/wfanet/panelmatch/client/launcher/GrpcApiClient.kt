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

import java.time.Clock
import kotlinx.coroutines.sync.Semaphore
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKt.debugLogEntry
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Party
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.RecurringExchangeParentKey
import org.wfanet.measurement.api.v2alpha.appendExchangeStepAttemptLogEntryRequest
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.finishExchangeStepAttemptRequest
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.launcher.ApiClient.ClaimedExchangeStep

class GrpcApiClient(
  identity: Identity,
  private val exchangeStepsClient: ExchangeStepsCoroutineStub,
  private val exchangeStepAttemptsClient: ExchangeStepAttemptsCoroutineStub,
  private val clock: Clock = Clock.systemUTC(),
  val maxClaimedExchangeSteps: Int,
) : ApiClient {
  private val semaphore =
    if (maxClaimedExchangeSteps != -1) Semaphore(maxClaimedExchangeSteps) else null

  private val recurringExchangeParentKey: RecurringExchangeParentKey =
    when (identity.party) {
      Party.DATA_PROVIDER -> DataProviderKey(identity.id)
      Party.MODEL_PROVIDER -> ModelProviderKey(identity.id)
      Party.PARTY_UNSPECIFIED,
      Party.UNRECOGNIZED -> throw IllegalArgumentException("Unsupported party ${identity.party}")
    }

  override suspend fun claimExchangeStep(): ClaimedExchangeStep? {
    if (semaphore !== null) {
      val semaphoreAcquired = semaphore.tryAcquire()
      if (!semaphoreAcquired) return null
    }
    val response: ClaimReadyExchangeStepResponse =
      exchangeStepsClient.claimReadyExchangeStep(
        claimReadyExchangeStepRequest { parent = recurringExchangeParentKey.toName() }
      )
    if (response.hasExchangeStep()) {
      val exchangeStepAttemptKey =
        grpcRequireNotNull(CanonicalExchangeStepAttemptKey.fromName(response.exchangeStepAttempt))
      return ClaimedExchangeStep(response.exchangeStep, exchangeStepAttemptKey)
    }
    return null
  }

  override suspend fun appendLogEntry(
    key: CanonicalExchangeStepAttemptKey,
    messages: Iterable<String>,
  ) {
    val request = appendExchangeStepAttemptLogEntryRequest {
      name = key.toName()
      for (message in messages) {
        logEntries += makeLogEntry(message)
      }
    }
    exchangeStepAttemptsClient.appendExchangeStepAttemptLogEntry(request)
  }

  override suspend fun finishExchangeStepAttempt(
    key: CanonicalExchangeStepAttemptKey,
    finalState: ExchangeStepAttempt.State,
    logEntryMessages: Iterable<String>,
  ) {
    val request = finishExchangeStepAttemptRequest {
      name = key.toName()
      this.finalState = finalState
      for (message in logEntryMessages) {
        logEntries += makeLogEntry(message)
      }
    }
    exchangeStepAttemptsClient.finishExchangeStepAttempt(request)
    if (semaphore !== null) semaphore.release()
  }

  private fun makeLogEntry(message: String): ExchangeStepAttempt.DebugLogEntry {
    return debugLogEntry {
      entryTime = clock.instant().toProtoTime()
      this.message = message
    }
  }
}
