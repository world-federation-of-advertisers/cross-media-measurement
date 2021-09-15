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
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_CERT_PEM_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_KEY_FILE
import org.wfanet.measurement.common.crypto.testing.KEY_ALGORITHM
import org.wfanet.panelmatch.client.launcher.testing.buildMockCryptor
import org.wfanet.panelmatch.client.launcher.testing.buildStep
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient
import org.wfanet.panelmatch.client.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class ExchangeTaskMapperForJoinKeyExchangeTest {
  private val privateStorage =
    VerifiedStorageClient(
      InMemoryStorageClient(keyPrefix = "private"),
      readCertificate(FIXED_SERVER_CERT_PEM_FILE),
      readCertificate(FIXED_SERVER_CERT_PEM_FILE),
      readPrivateKey(FIXED_SERVER_KEY_FILE, KEY_ALGORITHM)
    )
  private val sharedStorage =
    VerifiedStorageClient(
      InMemoryStorageClient(keyPrefix = "shared"),
      readCertificate(FIXED_SERVER_CERT_PEM_FILE),
      readCertificate(FIXED_SERVER_CERT_PEM_FILE),
      readPrivateKey(FIXED_SERVER_KEY_FILE, KEY_ALGORITHM)
    )
  private val deterministicCommutativeCryptor = buildMockCryptor()

  private val exchangeTaskMapper =
    ExchangeTaskMapperForJoinKeyExchange(
      deterministicCommutativeCryptor = deterministicCommutativeCryptor,
      sharedStorage = sharedStorage,
      privateStorage = privateStorage
    )

  @Test
  fun `map input task`() = runBlockingTest {
    val testStep =
      buildStep(ExchangeWorkflow.Step.StepCase.INPUT_STEP, privateOutputLabels = mapOf("a" to "b"))
    val exchangeTask: ExchangeTask = exchangeTaskMapper.getExchangeTaskForStep(testStep)
    assertThat(exchangeTask).isInstanceOf(InputTask::class.java)
  }

  @Test
  fun `map crypto task`() = runBlockingTest {
    val testStep = buildStep(ExchangeWorkflow.Step.StepCase.ENCRYPT_STEP)
    val exchangeTask: ExchangeTask = exchangeTaskMapper.getExchangeTaskForStep(testStep)
    assertThat(exchangeTask).isInstanceOf(CryptorExchangeTask::class.java)
  }
}
