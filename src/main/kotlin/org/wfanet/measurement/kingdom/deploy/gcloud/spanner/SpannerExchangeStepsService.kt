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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import java.time.Clock
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetailsKt.debugLog
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.StreamExchangeStepsRequest
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepResponse
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.PROVIDER_PARAM
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.providerFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamExchangeSteps
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepAttemptReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ClaimReadyExchangeStep
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ClaimReadyExchangeStep.Result
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateExchangesAndSteps
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.FinishExchangeStepAttempt
import org.wfanet.measurement.kingdom.service.internal.externalDataProviderId
import org.wfanet.measurement.kingdom.service.internal.externalModelProviderId

class SpannerExchangeStepsService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ExchangeStepsCoroutineImplBase() {

  override suspend fun getExchangeStep(request: GetExchangeStepRequest): ExchangeStep {
    val exchangeStepResult =
      ExchangeStepReader()
        .fillStatementBuilder {
          appendClause(
            """
            WHERE RecurringExchanges.ExternalRecurringExchangeId = @external_recurring_exchange_id
              AND ExchangeSteps.Date = @date
              AND ExchangeSteps.StepIndex = @step_index
              AND ${providerFilter(request.provider)}
          """
              .trimIndent()
          )
          bind("external_recurring_exchange_id" to request.externalRecurringExchangeId)
          bind("date" to request.date.toCloudDate())
          bind("step_index" to request.stepIndex.toLong())
          bind(PROVIDER_PARAM to request.provider.externalId)
          appendClause("LIMIT 1")
        }
        .execute(client.singleUse())
        .singleOrNull()
        ?: failGrpc(Status.NOT_FOUND) { "ExchangeStep not found" }

    return exchangeStepResult.exchangeStep
  }

  override suspend fun claimReadyExchangeStep(
    request: ClaimReadyExchangeStepRequest
  ): ClaimReadyExchangeStepResponse {
    val externalModelProviderId = request.provider.externalModelProviderId
    val externalDataProviderId = request.provider.externalDataProviderId

    CreateExchangesAndSteps(provider = request.provider).execute(client, idGenerator)

    // TODO(@efoxepstein): consider whether a more structured signal for auto-fail is needed
    val debugLogEntry = debugLog {
      message = "Automatically FAILED because of expiration"
      time = clock.instant().toProtoTime()
    }

    ExchangeStepAttemptReader.forExpiredAttempts(
        externalModelProviderId = externalModelProviderId,
        externalDataProviderId = externalDataProviderId,
        clock
      )
      .execute(client.singleUse())
      .map { it.exchangeStepAttempt }
      .collect { attempt: ExchangeStepAttempt ->
        FinishExchangeStepAttempt(
            provider = request.provider,
            externalRecurringExchangeId = ExternalId(attempt.externalRecurringExchangeId),
            exchangeDate = attempt.date,
            stepIndex = attempt.stepIndex,
            attemptNumber = attempt.attemptNumber,
            terminalState = ExchangeStepAttempt.State.FAILED,
            debugLogEntries = listOf(debugLogEntry),
            clock = clock
          )
          .execute(client, idGenerator)
      }

    val result =
      ClaimReadyExchangeStep(provider = request.provider, clock = clock)
        .execute(client, idGenerator)

    if (result.isPresent) {
      return result.get().toClaimReadyExchangeStepResponse()
    }

    return claimReadyExchangeStepResponse {}
  }

  override fun streamExchangeSteps(request: StreamExchangeStepsRequest): Flow<ExchangeStep> {
    return StreamExchangeSteps(request.filter, request.limit).execute(client.singleUse()).map {
      it.exchangeStep
    }
  }

  private fun Result.toClaimReadyExchangeStepResponse(): ClaimReadyExchangeStepResponse {
    return claimReadyExchangeStepResponse {
      this.exchangeStep = step
      this.attemptNumber = attemptIndex
    }
  }
}
