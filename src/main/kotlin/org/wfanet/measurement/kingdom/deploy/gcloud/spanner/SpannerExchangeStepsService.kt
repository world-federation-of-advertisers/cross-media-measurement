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
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetailsKt.debugLog
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Provider
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.finishExchangeStepAttemptRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepAttemptReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ClaimReadyExchangeStep
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ClaimReadyExchangeStep.Result
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateExchangesAndSteps
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.FinishExchangeStepAttempt

class SpannerExchangeStepsService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ExchangeStepsCoroutineImplBase() {

  override suspend fun claimReadyExchangeStep(
    request: ClaimReadyExchangeStepRequest
  ): ClaimReadyExchangeStepResponse {
    val (externalModelProviderId, externalDataProviderId) =
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (request.provider.type) {
        Provider.Type.DATA_PROVIDER -> Pair(null, request.provider.externalId)
        Provider.Type.MODEL_PROVIDER -> Pair(request.provider.externalId, null)
        Provider.Type.TYPE_UNSPECIFIED, Provider.Type.UNRECOGNIZED ->
          failGrpc(Status.INVALID_ARGUMENT) {
            "external_data_provider_id or external_model_provider_id must be provided."
          }
      }

    CreateExchangesAndSteps(
        externalModelProviderId = externalModelProviderId,
        externalDataProviderId = externalDataProviderId
      )
      .execute(client, idGenerator, clock)

    ExchangeStepAttemptReader.forExpiredAttempts(
        externalModelProviderId = externalModelProviderId,
        externalDataProviderId = externalDataProviderId
      )
      .execute(client.singleUse())
      .map { it.exchangeStepAttempt }
      .map { attempt ->
        finishExchangeStepAttemptRequest {
          externalRecurringExchangeId = attempt.externalRecurringExchangeId
          date = attempt.date
          stepIndex = attempt.stepIndex
          attemptNumber = attempt.attemptNumber
          state = ExchangeStepAttempt.State.FAILED
          provider = request.provider
          debugLogEntries +=
            debugLog {
              message = "Automatically FAILED because of expiration"
              time = clock.instant().toProtoTime()
            }
          // TODO(@efoxepstein): consider whether a more structured signal for auto-fail is needed
        }
      }
      .collect { FinishExchangeStepAttempt(it).execute(client) }

    val result =
      ClaimReadyExchangeStep(
          externalModelProviderId = externalModelProviderId,
          externalDataProviderId = externalDataProviderId
        )
        .execute(client, idGenerator, clock)

    if (result.isPresent) {
      return result.get().toClaimReadyExchangeStepResponse()
    }

    return ClaimReadyExchangeStepResponse.getDefaultInstance()
  }

  private fun Result.toClaimReadyExchangeStepResponse(): ClaimReadyExchangeStepResponse {
    return claimReadyExchangeStepResponse {
      this.exchangeStep = step
      this.attemptNumber = attemptIndex
    }
  }
}
