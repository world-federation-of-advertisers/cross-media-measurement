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

import java.time.Clock
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepRequest.PartyCase
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ClaimReadyExchangeStep
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ClaimReadyExchangeStep.Result
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateExchangesAndSteps

class SpannerExchangeStepsService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ExchangeStepsCoroutineImplBase() {

  override suspend fun claimReadyExchangeStep(
    request: ClaimReadyExchangeStepRequest
  ): ClaimReadyExchangeStepResponse {
    grpcRequire(request.partyCase != PartyCase.PARTY_NOT_SET) {
      "external_data_provider_id or external_model_provider_id must be provided."
    }
    val externalModelProviderId =
      if (request.hasExternalModelProviderId()) request.externalModelProviderId else null
    val externalDataProviderId =
      if (request.hasExternalDataProviderId()) request.externalDataProviderId else null

    CreateExchangesAndSteps(
        externalModelProviderId = externalModelProviderId,
        externalDataProviderId = externalDataProviderId
      )
      .execute(client, idGenerator, clock)

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
    return ClaimReadyExchangeStepResponse.newBuilder()
      .apply {
        this.exchangeStep = step
        this.attemptNumber = attemptIndex
      }
      .build()
  }
}
