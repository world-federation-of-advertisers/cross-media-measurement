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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Provider
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.provider

private const val FIXED_GENERATED_INTERNAL_ID = 2345L
private const val FIXED_GENERATED_EXTERNAL_ID = 6789L

@RunWith(JUnit4::class)
abstract class ExchangeStepsServiceTest<T : ExchangeStepsCoroutineImplBase> {

  protected val idGenerator =
    FixedIdGenerator(
      InternalId(FIXED_GENERATED_INTERNAL_ID),
      ExternalId(FIXED_GENERATED_EXTERNAL_ID)
    )

  protected abstract fun newService(idGenerator: IdGenerator): T

  private lateinit var exchangeStepsService: T

  @Before
  fun initService() {
    exchangeStepsService = newService(idGenerator)
  }

  @Test
  fun `claimReadyExchangeStepRequest fails for missing Provider id`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        exchangeStepsService.claimReadyExchangeStep(claimReadyExchangeStepRequest {})
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("external_data_provider_id or external_model_provider_id must be provided.")
  }

  @Test
  fun `claimReadyExchangeStepRequest fails without recurring exchange`() = runBlocking {
    val response =
      exchangeStepsService.claimReadyExchangeStep(
        claimReadyExchangeStepRequest {
          provider =
            provider {
              externalId = 6L
              type = Provider.Type.MODEL_PROVIDER
            }
        }
      )

    assertThat(response).isEqualTo(claimReadyExchangeStepResponse {})
  }

  @Test
  fun `claimReadyExchangeStepRequest succeeds`() = runBlocking {
    // TODO(yunyeng): Add test once underlying services complete.
  }

  @Test
  fun `claimReadyExchangeStepRequest succeeds with ready exchange step`() = runBlocking {
    // TODO(yunyeng): Add test once underlying services complete.
  }

  @Test
  fun `claimReadyExchangeStepRequest fails expired ExchangeStepAttempts`() = runBlocking {
    // TODO(yunyeng): Add test once underlying services complete.
  }
}
