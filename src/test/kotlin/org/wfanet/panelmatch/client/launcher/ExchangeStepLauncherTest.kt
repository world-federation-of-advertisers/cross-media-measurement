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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase as ExchangeStepsCoroutineService
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.api.v2alpha.FindReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.FindReadyExchangeStepResponse
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule

private const val DATA_PROVIDER_ID = "1"
private const val MODEL_PROVIDER_ID = "2"
private const val EXCHANGE_ID = "1"

private val EXCHANGE_STEP =
  ExchangeStep.newBuilder()
    .setState(ExchangeStep.State.READY)
    .setKey(ExchangeStep.Key.newBuilder().setExchangeId(EXCHANGE_ID))
    .build()
private val REQUEST_WITH_DATA_PROVIDER =
  FindReadyExchangeStepRequest.newBuilder()
    .apply { dataProviderBuilder.dataProviderId = DATA_PROVIDER_ID }
    .build()
private val REQUEST_WITH_MODEL_PROVIDER =
  FindReadyExchangeStepRequest.newBuilder()
    .apply { modelProviderBuilder.modelProviderId = MODEL_PROVIDER_ID }
    .build()
private val RESPONSE =
  FindReadyExchangeStepResponse.newBuilder().setExchangeStep(EXCHANGE_STEP).build()
private val EMPTY_RESPONSE = FindReadyExchangeStepResponse.newBuilder().build()

@RunWith(JUnit4::class)
class ExchangeStepLauncherTest {

  private val exchangeStepsServiceMock: ExchangeStepsCoroutineService =
    mock(useConstructor = UseConstructor.parameterless())
  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(exchangeStepsServiceMock) }

  private val exchangeStepsStub: ExchangeStepsCoroutineStub by lazy {
    ExchangeStepsCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `findExchangeStep with dataProvider`() {
    val launcher =
      ExchangeStepLauncher(
        exchangeStepsClient = exchangeStepsStub,
        id = DATA_PROVIDER_ID,
        partyType = PartyType.DATA_PROVIDER,
        clock = Clock.systemUTC()
      )
    runBlocking {
      whenever(exchangeStepsServiceMock.findReadyExchangeStep(any())).thenReturn(RESPONSE)
      val exchangeStep = launcher.findExchangeStep()
      assertThat(exchangeStep).isEqualTo(EXCHANGE_STEP)
      verify(exchangeStepsServiceMock, times(1)).findReadyExchangeStep(REQUEST_WITH_DATA_PROVIDER)
    }
  }

  @Test
  fun `findExchangeStep with modelProvider`() {
    val launcher =
      ExchangeStepLauncher(
        exchangeStepsClient = exchangeStepsStub,
        id = MODEL_PROVIDER_ID,
        partyType = PartyType.MODEL_PROVIDER,
        clock = Clock.systemUTC()
      )
    runBlocking {
      whenever(exchangeStepsServiceMock.findReadyExchangeStep(any())).thenReturn(RESPONSE)
      val exchangeStep = launcher.findExchangeStep()
      assertThat(exchangeStep).isEqualTo(EXCHANGE_STEP)
      verify(exchangeStepsServiceMock, times(1)).findReadyExchangeStep(REQUEST_WITH_MODEL_PROVIDER)
    }
  }

  @Test
  fun `findExchangeStep without exchangeStep`() = runBlocking {
    val launcher =
      ExchangeStepLauncher(
        exchangeStepsClient = exchangeStepsStub,
        id = DATA_PROVIDER_ID,
        partyType = PartyType.DATA_PROVIDER,
        clock = Clock.systemUTC()
      )
    whenever(exchangeStepsServiceMock.findReadyExchangeStep(any())).thenReturn(EMPTY_RESPONSE)
    val exchangeStep = launcher.findExchangeStep()
    assertThat(exchangeStep).isNull()
  }
}
