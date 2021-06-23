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

package org.wfanet.panelmatch.client.exchangetasks

import com.google.common.truth.Truth.assertThat
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.panelmatch.client.launcher.ApiClient
import org.wfanet.panelmatch.client.launcher.testing.buildMockCryptor
import org.wfanet.panelmatch.client.storage.InMemoryStorage

@RunWith(JUnit4::class)
class ExchangeTaskMapperForJoinKeyExchangeTest {
  private val apiClient: ApiClient = mock()
  private val privateStorage = InMemoryStorage(keyPrefix = "private")
  private val sharedStorage = InMemoryStorage(keyPrefix = "shared")
  private val retryDuration: Duration = Duration.ofMillis(500)
  private val deterministicCommutativeCryptor = buildMockCryptor()

  @Test
  fun `map input task`() =
    runBlocking<Unit> {
      val testStep =
        ExchangeWorkflow.Step.newBuilder()
          .apply { inputStep = ExchangeWorkflow.Step.InputStep.getDefaultInstance() }
          .build()
      val exchangeTask: ExchangeTask =
        ExchangeTaskMapperForJoinKeyExchange(
            deterministicCommutativeCryptor = deterministicCommutativeCryptor,
            retryDuration = retryDuration,
            sharedStorage = sharedStorage,
            privateStorage = privateStorage
          )
          .getExchangeTaskForStep(testStep)
      assertThat(exchangeTask).isInstanceOf(InputTask::class.java)
    }

  @Test
  fun `map crypto task`() =
    runBlocking<Unit> {
      val testStep =
        ExchangeWorkflow.Step.newBuilder()
          .apply { encryptStep = ExchangeWorkflow.Step.EncryptStep.getDefaultInstance() }
          .build()
      val exchangeTask: ExchangeTask =
        ExchangeTaskMapperForJoinKeyExchange(
            deterministicCommutativeCryptor = deterministicCommutativeCryptor,
            retryDuration = retryDuration,
            sharedStorage = sharedStorage,
            privateStorage = privateStorage
          )
          .getExchangeTaskForStep(testStep)
      assertThat(exchangeTask).isInstanceOf(CryptorExchangeTask::class.java)
    }
}
