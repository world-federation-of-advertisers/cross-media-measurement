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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.StepKt.encryptStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.step
import org.wfanet.panelmatch.client.launcher.testing.inputStep
import org.wfanet.panelmatch.client.privatemembership.testing.PlaintextPrivateMembershipCryptor
import org.wfanet.panelmatch.client.storage.testing.makeTestVerifiedStorageClient
import org.wfanet.panelmatch.common.crypto.testing.FakeDeterministicCommutativeCipher
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class ExchangeTaskMapperForJoinKeyExchangeTest {
  private val privateStorage = makeTestVerifiedStorageClient()

  private val exchangeTaskMapper =
    ExchangeTaskMapperForJoinKeyExchange(
      getDeterministicCommutativeCryptor = ::FakeDeterministicCommutativeCipher,
      getPrivateMembershipCryptor = ::PlaintextPrivateMembershipCryptor,
      privateStorage = privateStorage
    )

  @Test
  fun `map input task`() = runBlockingTest {
    val testStep = inputStep("a" to "b")
    val exchangeTask: ExchangeTask = exchangeTaskMapper.getExchangeTaskForStep(testStep)
    assertThat(exchangeTask).isInstanceOf(InputTask::class.java)
  }

  @Test
  fun `map crypto task`() = runBlockingTest {
    val testStep = step { encryptStep = encryptStep {} }
    val exchangeTask: ExchangeTask = exchangeTaskMapper.getExchangeTaskForStep(testStep)
    assertThat(exchangeTask).isInstanceOf(CryptorExchangeTask::class.java)
  }
}
