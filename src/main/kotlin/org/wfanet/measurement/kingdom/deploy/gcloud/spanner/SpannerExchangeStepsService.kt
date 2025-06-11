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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetailsKt.debugLog
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.GetExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.StreamExchangeStepsRequest
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepResponse
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ExchangeStepNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamExchangeSteps
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepAttemptReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ClaimReadyExchangeStep
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ClaimReadyExchangeStep.Result
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateExchangesAndSteps
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.FinishExchangeStepAttempt

class SpannerExchangeStepsService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ExchangeStepsCoroutineImplBase(coroutineContext) {

  override suspend fun getExchangeStep(request: GetExchangeStepRequest): ExchangeStep {
    val externalRecurringExchangeId = ExternalId(request.externalRecurringExchangeId)
    val exchangeStepResult =
      ExchangeStepReader()
        .readByExternalIds(
          client.singleUse(),
          externalRecurringExchangeId,
          request.date,
          request.stepIndex,
        )
        ?: throw ExchangeStepNotFoundException(
            externalRecurringExchangeId,
            request.date,
            request.stepIndex,
          )
          .asStatusRuntimeException(Status.Code.NOT_FOUND, "ExchangeStep not found")

    return exchangeStepResult.exchangeStep
  }

  override suspend fun claimReadyExchangeStep(
    request: ClaimReadyExchangeStepRequest
  ): ClaimReadyExchangeStepResponse {
    val (party, externalPartyId) =
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf oneof case cannot be null.
      when (request.partyCase) {
        ClaimReadyExchangeStepRequest.PartyCase.EXTERNAL_DATA_PROVIDER_ID ->
          ExchangeWorkflow.Party.DATA_PROVIDER to ExternalId(request.externalDataProviderId)
        ClaimReadyExchangeStepRequest.PartyCase.EXTERNAL_MODEL_PROVIDER_ID ->
          ExchangeWorkflow.Party.MODEL_PROVIDER to ExternalId(request.externalModelProviderId)
        ClaimReadyExchangeStepRequest.PartyCase.PARTY_NOT_SET ->
          throw Status.INVALID_ARGUMENT.withDescription("party not set").asRuntimeException()
      }

    CreateExchangesAndSteps(party, externalPartyId).execute(client, idGenerator)

    // TODO(@efoxepstein): consider whether a more structured signal for auto-fail is needed
    val debugLogEntry = debugLog {
      message = "Automatically FAILED because of expiration"
      time = clock.instant().toProtoTime()
    }

    ExchangeStepAttemptReader.forExpiredAttempts(party, externalPartyId, clock)
      .execute(client.singleUse())
      .map { it.exchangeStepAttempt }
      .collect { attempt: ExchangeStepAttempt ->
        FinishExchangeStepAttempt(
            externalRecurringExchangeId = ExternalId(attempt.externalRecurringExchangeId),
            exchangeDate = attempt.date,
            stepIndex = attempt.stepIndex,
            attemptNumber = attempt.attemptNumber,
            terminalState = ExchangeStepAttempt.State.FAILED,
            debugLogEntries = listOf(debugLogEntry),
            clock = clock,
          )
          .execute(client, idGenerator)
      }

    val result =
      ClaimReadyExchangeStep(request = request, clock = clock).execute(client, idGenerator)

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
